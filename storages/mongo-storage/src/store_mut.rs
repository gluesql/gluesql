use {
    crate::{
        error::{MongoStorageError, OptionExt, ResultExt},
        row::{
            data_type::{BsonType, IntoRange},
            key::{into_object_id, KeyIntoBson},
            value::IntoBson,
        },
        utils::get_primary_key,
        MongoStorage,
    },
    async_trait::async_trait,
    gluesql_core::{
        ast::{ColumnUniqueOption, ToSql},
        data::{Key, Schema},
        error::Result,
        prelude::Error,
        store::{DataRow, StoreMut},
    },
    mongodb::{
        bson::{doc, Bson, Document},
        options::{CreateCollectionOptions, IndexOptions, ReplaceOptions},
    },
};

struct IndexInfo {
    name: String, // TODO: convert to enum with primary and unique
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
        let (names, column_types, indexes) = schema
            .column_defs
            .as_ref()
            .map(|column_defs| {
                column_defs.iter().fold(
                    (Vec::new(), Document::new(), Vec::new()),
                    |(mut names, mut column_types, mut indexes), column_def| {
                        let column_name = &column_def.name;
                        names.push(column_name.clone());

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

                        if let Some(default) = &column_def.default {
                            property.extend(doc! {
                                "description": default.to_sql()
                            });
                        }

                        let type_str = column_def.data_type.to_string();
                        property.extend(doc! {
                            "title": type_str
                        });

                        let column_type = doc! {
                            column_name: property,
                        };

                        column_types.extend(column_type);

                        (names, column_types, indexes)
                    },
                )
            })
            .unwrap_or_default();

        let mut properties = doc! {
            "_id": { "bsonType": ["objectId", "binData"] }
        };
        properties.extend(column_types);
        let mut required = vec!["_id".to_string()];
        required.extend(names.clone());

        let additional_properties = matches!(names.len(), 0);

        let option = CreateCollectionOptions::builder()
            .validator(Some(doc! {
                "$jsonSchema": {
                    "type": "object",
                    "required": required,
                    "properties": properties,
                    "additionalProperties": additional_properties
                  }
            }))
            .build();

        self.db
            .create_collection(&schema.table_name, option)
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
                    .fold(Ok(Document::new()), |acc, (column_def, value)| {
                        let mut acc = acc?;
                        acc.extend(doc! {column_def.name.clone(): value.into_bson()?});

                        Ok(acc)
                    }),
                DataRow::Map(hash_map) => {
                    hash_map
                        .into_iter()
                        .fold(Ok(Document::new()), |acc, (key, value)| {
                            let mut acc = acc?;
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
                    .fold(
                        Ok::<_, Error>(doc! {"_id": key.clone().into_bson(primary_key.is_some())?}),
                        |acc, (column_def, value)| {
                            let mut acc = acc.map_storage_err()?;
                            acc.extend(doc! {column_def.name.clone(): value.into_bson()?});

                            Ok(acc)
                        },
                    ),
                DataRow::Map(hash_map) => hash_map.into_iter().fold(
                    Ok(doc! {"_id": into_object_id(key.clone())?}),
                    |acc, (key, value)| {
                        let mut acc = acc?;
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
