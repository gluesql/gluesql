use {
    crate::{
        description::ColumnDescription,
        error::{MongoStorageError, OptionExt, ResultExt},
        row::{
            data_type::{BsonType, IntoRange},
            key::{into_object_id, KeyIntoBson},
            value::IntoBson,
        },
        utils::{get_primary_key, Validator},
        MongoStorage,
    },
    async_trait::async_trait,
    gluesql_core::{
        ast::ColumnUniqueOption,
        data::{Key, Schema},
        error::{Error, Result},
        store::{DataRow, Store, StoreMut},
    },
    mongodb::{
        bson::{doc, Bson, Document},
        options::{IndexOptions, ReplaceOptions},
    },
};

struct IndexInfo {
    name: String,
    key: String,
    index_type: IndexType,
}

enum IndexType {
    Primary,
    Unique,
}

#[async_trait(?Send)]
impl StoreMut for MongoStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let (labels, column_types, indexes) = schema
            .column_defs
            .as_ref()
            .map(|column_defs| {
                column_defs.iter().try_fold(
                    (Vec::new(), Document::new(), Vec::new()),
                    |(mut labels, mut column_types, mut indexes), column_def| {
                        let column_name = &column_def.name;
                        labels.push(column_name.clone());

                        let data_type = BsonType::from(&column_def.data_type).into();
                        let maximum = column_def.data_type.get_max();
                        let minimum = column_def.data_type.get_min();

                        let mut bson_type = match column_def.nullable {
                            true => vec![data_type, "null"],
                            false => vec![data_type],
                        };

                        match &column_def.unique {
                            Some(ColumnUniqueOption { is_primary }) => match *is_primary {
                                true => {
                                    indexes.push(IndexInfo {
                                        name: format!("{column_name}_PK"),
                                        key: column_name.clone(),
                                        index_type: IndexType::Primary,
                                    });
                                }
                                false => {
                                    bson_type = vec![data_type, "null"];
                                    indexes.push(IndexInfo {
                                        name: format!("{column_name}_UNIQUE"),
                                        key: column_name.clone(),
                                        index_type: IndexType::Unique,
                                    });
                                }
                            },
                            None => {}
                        }

                        let mut property = doc! {
                            "bsonType": bson_type,
                        };

                        if let Some(maximum) = maximum {
                            property.extend(doc! {
                                "maximum": maximum,
                            });
                        }

                        if let Some(minimum) = minimum {
                            property.extend(doc! {
                                "minimum": minimum,
                            });
                        }

                        let column_description = ColumnDescription {
                            default: column_def.default.clone(),
                            comment: column_def.comment.clone(),
                        };
                        let column_description =
                            serde_json::to_string(&column_description).map_storage_err()?;

                        property.extend(doc! {
                            "description": column_description,
                        });

                        let type_str = column_def.data_type.to_string();
                        property.extend(doc! {
                            "title": type_str
                        });

                        let column_type = doc! {
                            column_name: property,
                        };

                        column_types.extend(column_type);

                        Ok::<_, Error>((labels, column_types, indexes))
                    },
                )
            })
            .transpose()?
            .unwrap_or_default();

        let comment = schema.comment.as_ref().map(ToOwned::to_owned);
        let validator = Validator::new(labels, column_types, schema.foreign_keys.clone(), comment)?;

        let schema_exists = self
            .fetch_schema(&schema.table_name)
            .await
            .map_storage_err()?
            .is_some();

        if schema_exists {
            let command = doc! {
                "collMod": schema.table_name.clone(),
                "validator": validator.document,
                "validationLevel": "strict",
                "validationAction": "error",
            };
            self.db.run_command(command, None).await.map_storage_err()?;

            return Ok(());
        }

        let options = validator.to_options();

        self.db
            .create_collection(&schema.table_name, options)
            .await
            .map_storage_err()?;

        if indexes.is_empty() {
            return Ok(());
        }

        let index_models = indexes
            .into_iter()
            .map(
                |IndexInfo {
                     name,
                     key,
                     index_type,
                 }| {
                    let index_options = IndexOptions::builder().unique(true);
                    let index_options = match index_type {
                        IndexType::Primary => index_options.name(name).build(),
                        IndexType::Unique => index_options
                            .partial_filter_expression(
                                doc! { "partialFilterExpression": { key.clone(): { "$ne": null } } }, 
                            )
                            .name(name)
                            .build(),
                    };

                    mongodb::IndexModel::builder()
                        .keys(doc! {key: 1})
                        .options(index_options)
                        .build()
                },
            )
            .collect::<Vec<_>>();

        self.db
            .collection::<Document>(&schema.table_name)
            .create_indexes(index_models, None)
            .await
            .map(|_| ())
            .map_storage_err()
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.db
            .collection::<Document>(table_name)
            .drop(None)
            .await
            .map_storage_err()
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        let column_defs = self.get_column_defs(table_name).await?;

        let data = rows
            .into_iter()
            .map(|row| match row {
                DataRow::Vec(values) => column_defs
                    .as_ref()
                    .map_storage_err(MongoStorageError::Unreachable)?
                    .iter()
                    .zip(values.into_iter())
                    .try_fold(Document::new(), |mut acc, (column_def, value)| {
                        acc.extend(doc! {column_def.name.clone(): value.into_bson()?});

                        Ok(acc)
                    }),
                DataRow::Map(hash_map) => {
                    hash_map
                        .into_iter()
                        .try_fold(Document::new(), |mut acc, (key, value)| {
                            acc.extend(doc! {key: value.into_bson()?});

                            Ok(acc)
                        })
                }
            })
            .collect::<Result<Vec<_>>>()?;

        if data.is_empty() {
            return Ok(());
        }

        self.db
            .collection::<Document>(table_name)
            .insert_many(data, None)
            .await
            .map(|_| ())
            .map_storage_err()
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        let column_defs = self.get_column_defs(table_name).await?;

        let primary_key = column_defs
            .as_ref()
            .and_then(|column_defs| get_primary_key(column_defs));

        for (key, row) in rows {
            let doc = match row {
                DataRow::Vec(values) => column_defs
                    .as_ref()
                    .map_storage_err(MongoStorageError::Unreachable)?
                    .iter()
                    .zip(values.into_iter())
                    .try_fold(
                        doc! {"_id": key.clone().into_bson(primary_key.is_some())?},
                        |mut acc, (column_def, value)| {
                            acc.extend(doc! {column_def.name.clone(): value.into_bson()?});

                            Ok::<_, Error>(acc)
                        },
                    ),
                DataRow::Map(hash_map) => hash_map.into_iter().try_fold(
                    doc! {"_id": into_object_id(key.clone())?},
                    |mut acc, (key, value)| {
                        acc.extend(doc! {key: value.into_bson()?});

                        Ok(acc)
                    },
                ),
            }?;

            let query = doc! {"_id": key.into_bson(primary_key.is_some())?};
            let options = ReplaceOptions::builder().upsert(Some(true)).build();

            self.db
                .collection::<Document>(table_name)
                .replace_one(query, doc, options)
                .await
                .map_storage_err()?;
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let column_defs = self.get_column_defs(table_name).await?;
        let primary_key = column_defs
            .as_ref()
            .and_then(|column_defs| get_primary_key(column_defs));

        self.db
            .collection::<Bson>(table_name)
            .delete_many(
                doc! { "_id": {
                    "$in": keys.into_iter().map(|key| key.into_bson(primary_key.is_some())).collect::<Result<Vec<_>>>()?
                }},
                None,
            )
            .await
            .map(|_| ())
            .map_storage_err()
    }
}
