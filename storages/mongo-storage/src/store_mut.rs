use {
    crate::{
        description::ColumnDescription,
        error::{MongoStorageError, OptionExt, ResultExt},
        row::{
            data_type::{BsonType, IntoRange},
            key::{into_object_id, KeyIntoBson},
            value::IntoBson,
        },
        utils::{get_primary_key, get_primary_key_sort_document, has_primary_key, Validator},
        MongoStorage, PRIMARY_KEY_DESINENCE, UNIQUE_KEY_DESINENCE,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::{Error, Result},
        store::{DataRow, Store, StoreMut},
    },
    mongodb::{
        bson::{doc, Bson, Document},
        options::{IndexOptions, ReplaceOptions},
    },
};

#[async_trait(?Send)]
impl StoreMut for MongoStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let (labels, column_types, unique_indexes, primary_indexes) = schema
            .column_defs
            .as_ref()
            .map(|column_defs| {
                column_defs.iter().try_fold(
                    (Vec::new(), Document::new(), Vec::new(), Vec::new()),
                    |(mut labels, mut column_types, mut unique_indexes, mut primary_indexes),
                     column_def| {
                        labels.push(column_def.name.clone());

                        let data_type = BsonType::from(&column_def.data_type).into();
                        let maximum = column_def.data_type.get_max();
                        let minimum = column_def.data_type.get_min();

                        let bson_type =
                            match column_def.nullable || column_def.is_unique_not_primary() {
                                true => vec![data_type, crate::NULLABLE_SYMBOL],
                                false => vec![data_type],
                            };

                        if column_def.is_primary() {
                            primary_indexes.push(column_def);
                        } else if column_def.is_unique_not_primary() {
                            unique_indexes.push(column_def);
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

                        column_types.extend(doc! {
                            column_def.name.clone(): property,
                        });

                        Ok::<_, Error>((labels, column_types, unique_indexes, primary_indexes))
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

        if primary_indexes.is_empty() && unique_indexes.is_empty() {
            return Ok(());
        }

        let mut index_models = unique_indexes
            .into_iter()
            .map(|column_def| {
                let index_options = IndexOptions::builder()
                    .unique(true)
                    .partial_filter_expression(
                        doc! { "partialFilterExpression": { column_def.name.clone(): { "$ne": null } } },
                    )
                    .name(format!("{}_{}", column_def.name, UNIQUE_KEY_DESINENCE))
                    .build();

                mongodb::IndexModel::builder()
                    .keys(doc! {column_def.name.clone(): 1})
                    .options(index_options)
                    .build()
            })
            .collect::<Vec<_>>();

        // If there is a primary key, we create a composite unique index with the primary key
        // where it is triggered solely when all of the columns in the primary key result
        // to be non-unique.
        if !primary_indexes.is_empty() {
            let options = IndexOptions::builder()
                .unique(true)
                .name(format!("{}_{}", schema.table_name, PRIMARY_KEY_DESINENCE))
                .build();

            index_models.push(
                mongodb::IndexModel::builder()
                    .keys(get_primary_key_sort_document(primary_indexes).unwrap())
                    .options(options)
                    .build(),
            );
        }

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

        let has_primary_key = column_defs
            .as_ref()
            .map(|column_defs| has_primary_key(column_defs))
            .unwrap_or_default();

        for (key, row) in rows {
            let doc = match row {
                DataRow::Vec(values) => column_defs
                    .as_ref()
                    .map_storage_err(MongoStorageError::Unreachable)?
                    .iter()
                    .zip(values.into_iter())
                    .try_fold(
                        doc! {crate::PRIMARY_KEY_SYMBOL: key.clone().into_bson(has_primary_key)?},
                        |mut acc, (column_def, value)| {
                            acc.extend(doc! {column_def.name.clone(): value.into_bson()?});

                            Ok::<_, Error>(acc)
                        },
                    ),
                DataRow::Map(hash_map) => hash_map.into_iter().try_fold(
                    doc! {crate::PRIMARY_KEY_SYMBOL: into_object_id(key.clone())?},
                    |mut acc, (key, value)| {
                        acc.extend(doc! {key: value.into_bson()?});

                        Ok(acc)
                    },
                ),
            }?;

            let query = doc! {crate::PRIMARY_KEY_SYMBOL: key.into_bson(has_primary_key)?};
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
                doc! { crate::PRIMARY_KEY_SYMBOL: {
                    "$in": keys.into_iter().map(|key| key.into_bson(primary_key.is_some())).collect::<Result<Vec<_>>>()?
                }},
                None,
            )
            .await
            .map(|_| ())
            .map_storage_err()
    }
}
