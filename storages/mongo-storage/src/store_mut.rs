use {
    crate::{
        description::ColumnDescription,
        error::{MongoStorageError, OptionExt, ResultExt},
        row::{
            data_type::{BsonType, IntoRange},
            key::{into_object_id, KeyIntoBson},
            value::IntoBson,
        },
        utils::Validator,
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
        let (labels, column_types) = schema
            .column_defs
            .as_ref()
            .map(|column_defs| {
                column_defs.iter().try_fold(
                    (Vec::new(), Document::new()),
                    |(mut labels, mut column_types), column_def| {
                        labels.push(column_def.name.clone());

                        let data_type = BsonType::from(&column_def.data_type).into();
                        let maximum = column_def.data_type.get_max();
                        let minimum = column_def.data_type.get_min();

                        let bson_type = match column_def.nullable
                            || schema.is_part_of_single_field_unique_constraint(&column_def.name)
                        {
                            true => vec![data_type, crate::NULLABLE_SYMBOL],
                            false => vec![data_type],
                        };

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

                        Ok::<_, Error>((labels, column_types))
                    },
                )
            })
            .transpose()?
            .unwrap_or_default();

        let comment = schema.comment.as_ref().map(ToOwned::to_owned);
        let validator = Validator::new(
            labels,
            column_types,
            schema.foreign_keys.clone(),
            schema.primary_key.clone(),
            schema.unique_constraints.clone(),
            comment,
        )?;

        let schema_exists = self.fetch_schema(&schema.table_name).await?.is_some();

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

        let mut index = 0;
        let mut index_models = schema
            .unique_constraint_columns_and_indices()
            .zip(schema.unique_constraints.iter())
            .map(|((_, column_names), unique_constraint)| {
                let constraint_name = unique_constraint
                    .name()
                    .map(|name| format!("{}_{}", schema.table_name, name))
                    .unwrap_or_else(|| format!("{}_{}", schema.table_name, index));

                index += 1;

                let partial_filter_expression = column_names.iter().map(ToOwned::to_owned).fold(
                    Document::new(),
                    |mut document, column_name| {
                        document.insert(column_name, doc! { "$ne": null });
                        document
                    },
                );

                let index_options = IndexOptions::builder()
                    .unique(true)
                    .partial_filter_expression(
                        doc! { "partialFilterExpression": partial_filter_expression },
                    )
                    .name(format!("{}_{}", &constraint_name, UNIQUE_KEY_DESINENCE))
                    .build();

                mongodb::IndexModel::builder()
                    .keys(column_names.into_iter().fold(
                        Document::new(),
                        |mut document, column_name| {
                            document.insert(column_name, 1);
                            document
                        },
                    ))
                    .options(index_options)
                    .build()
            })
            .collect::<Vec<_>>();

        // If there is a primary key, we create a composite unique index with the primary key
        // where it is triggered solely when all of the columns in the primary key result
        // to be non-unique.
        if let Some(primary_keys) = schema.primary_key_column_names() {
            let options = IndexOptions::builder()
                .unique(true)
                .name(format!("{}_{}", schema.table_name, PRIMARY_KEY_DESINENCE))
                .build();

            index_models.push(
                mongodb::IndexModel::builder()
                    .keys(
                        primary_keys.fold(Document::new(), |mut document, column_name| {
                            document.insert(column_name, 1);
                            document
                        }),
                    )
                    .options(options)
                    .build(),
            );
        }

        if !index_models.is_empty() {
            self.db
                .collection::<Document>(&schema.table_name)
                .create_indexes(index_models, None)
                .await
                .map_storage_err()?;
        }

        Ok(())
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
        let schema = self.fetch_schema(table_name).await?;

        let has_primary_key = schema
            .as_ref()
            .map_or(false, |schema| schema.primary_key.is_some());

        for (key, row) in rows {
            let doc = match row {
                DataRow::Vec(values) => schema
                    .as_ref()
                    .map_storage_err(MongoStorageError::Unreachable)?
                    .column_defs
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
        let has_primary_key = self
            .fetch_schema(table_name)
            .await?
            .as_ref()
            .map_or(false, |schema| schema.primary_key.is_some());

        self.db
            .collection::<Bson>(table_name)
            .delete_many(
                doc! { crate::PRIMARY_KEY_SYMBOL: {
                    "$in": keys.into_iter().map(|key| key.into_bson(has_primary_key)).collect::<Result<Vec<_>>>()?
                }},
                None,
            )
            .await
            .map(|_| ())
            .map_storage_err()
    }
}
