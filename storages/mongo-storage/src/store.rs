use {
    crate::{
        description::{ColumnDescription, TableDescription},
        error::{MongoStorageError, OptionExt, ResultExt},
        row::{key::KeyIntoBson, value::IntoValue, IntoRow},
        MongoStorage,
    },
    async_trait::async_trait,
    futures::{stream, Stream, StreamExt, TryStreamExt},
    gluesql_core::{
        ast::ColumnDef,
        data::{Key, Schema},
        error::Result,
        parse_sql::parse_data_type,
        prelude::{Error, Value},
        store::{DataRow, RowIter, Store},
        translate::translate_data_type,
    },
    mongodb::{
        bson::{doc, document::ValueAccessError, Document},
        options::FindOptions,
    },
    serde_json::from_str,
    std::collections::HashMap,
};

#[async_trait(?Send)]
impl Store for MongoStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.fetch_schemas_iter(Some(table_name))
            .await?
            .next()
            .await
            .transpose()
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut schemas = self
            .fetch_schemas_iter(None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        let schema = self
            .fetch_schema(table_name)
            .await?
            .map_storage_err(MongoStorageError::Unreachable)?;

        let column_defs = schema
            .column_defs
            .as_ref()
            .map_storage_err(MongoStorageError::Unreachable)?;

        let filter = doc! { crate::PRIMARY_KEY_SYMBOL: target.to_owned().into_bson(true)?};
        let projection = doc! {crate::PRIMARY_KEY_SYMBOL: 0};
        let options = FindOptions::builder()
            .projection(projection)
            .sort(schema.primary_key_column_names().map(|pk| {
                pk.fold(Document::new(), |mut document, column_name| {
                    document.insert(column_name, 1);
                    document
                })
            }))
            .build();

        let mut cursor = self
            .db
            .collection::<Document>(table_name)
            .find(filter, options)
            .await
            .map_storage_err()?;

        cursor
            .next()
            .await
            .transpose()
            .map_storage_err()?
            .map(|doc| {
                doc.into_iter()
                    .zip(column_defs.iter())
                    .map(|((_, bson), column_def)| bson.into_value(&column_def.data_type))
                    .collect::<Result<Vec<_>>>()
                    .map(DataRow::Vec)
            })
            .transpose()
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let schema = self.fetch_schema(table_name).await?;

        let primary_key_documnt = schema.as_ref().and_then(|schema| {
            schema.primary_key_column_names().map(|pk| {
                pk.fold(Document::new(), |mut document, column_name| {
                    document.insert(column_name, 1);
                    document
                })
            })
        });

        let has_primary = primary_key_documnt.is_some();

        let options = FindOptions::builder().sort(primary_key_documnt).build();

        let cursor = self
            .db
            .collection::<Document>(table_name)
            .find(Document::new(), options)
            .await
            .map_storage_err()?;

        let column_types = schema.as_ref().and_then(|schema| {
            schema.column_defs.as_ref().map(|column_defs| {
                column_defs
                    .iter()
                    .map(|column_def| column_def.data_type.clone())
                    .collect::<Vec<_>>()
            })
        });

        let row_iter = cursor.map(move |doc| {
            let doc = doc.map_storage_err()?;

            match &column_types {
                Some(column_types) => doc.into_row(column_types.iter(), has_primary),
                None => {
                    let mut iter = doc.into_iter();
                    let (_, first_value) = iter
                        .next()
                        .map_storage_err(MongoStorageError::InvalidDocument)?;
                    let key_bytes = first_value
                        .as_object_id()
                        .map_storage_err(MongoStorageError::InvalidDocument)?
                        .bytes()
                        .to_vec();
                    let key = Key::Bytea(key_bytes);
                    let row = iter
                        .map(|(key, bson)| Ok((key, bson.into_value_schemaless()?)))
                        .collect::<Result<HashMap<String, Value>>>()?;

                    Ok((key, DataRow::Map(row)))
                }
            }
        });

        Ok(Box::pin(row_iter))
    }
}

impl MongoStorage {
    async fn fetch_schemas_iter<'a>(
        &'a self,
        table_name: Option<&'a str>,
    ) -> Result<impl Stream<Item = Result<Schema>> + 'a> {
        let command = match table_name {
            Some(table_name) => doc! { "listCollections": 1, "filter": { "name": table_name } },
            None => doc! { "listCollections": 1 },
        };

        let validators_list = self
            .db
            .run_command(command, None)
            .await
            .map_storage_err()?
            .get_document("cursor")
            .and_then(|doc| doc.get_array("firstBatch"))
            .map_storage_err()?
            .to_owned();

        let schemas = stream::iter(validators_list).then(move |validators| async move {
            let doc = validators
                .as_document()
                .map_storage_err(MongoStorageError::InvalidDocument)?;

            let collection_name = doc.get_str("name").map_storage_err()?;
            let validator = doc
                .get_document("options")
                .and_then(|doc| doc.get_document("validator"))
                .and_then(|doc| doc.get_document("$jsonSchema"))
                .map_storage_err()?;

            let column_defs = validator
                .get_document("properties")
                .map_storage_err()?
                .into_iter()
                .skip(1)
                .map(|(column_name, doc)| {
                    let doc = doc
                        .as_document()
                        .map_storage_err(MongoStorageError::InvalidDocument)?;

                    let nullable = doc
                        .get_array("bsonType")
                        .map_err(|_| MongoStorageError::InvalidBsonType)
                        .map_storage_err()?
                        .get(1)
                        .and_then(|x| x.as_str())
                        .map(|x| x == crate::NULLABLE_SYMBOL)
                        .unwrap_or(false);

                    let data_type = doc
                        .get_str("title")
                        .map_err(|_| MongoStorageError::InvalidGlueType)
                        .map_storage_err()
                        .and_then(parse_data_type)
                        .and_then(|s| translate_data_type(&s))?;

                    let column_description = doc.get_str("description");
                    let ColumnDescription { default, comment } = match column_description {
                        Ok(desc) => {
                            serde_json::from_str::<ColumnDescription>(desc).map_storage_err()?
                        }
                        Err(ValueAccessError::NotPresent) => ColumnDescription {
                            default: None,
                            comment: None,
                        },
                        Err(_) => {
                            return Err(Error::StorageMsg(
                                MongoStorageError::InvalidGlueType.to_string(),
                            ))
                        }
                    };

                    let column_def = ColumnDef {
                        name: column_name.to_owned(),
                        data_type,
                        nullable,
                        default,
                        comment,
                    };

                    Ok(column_def)
                })
                .collect::<Result<Vec<ColumnDef>>>()?;

            let column_defs = match column_defs.len() {
                0 => None,
                _ => Some(column_defs),
            };

            let table_description = validator.get_str("description").map_storage_err()?;
            let TableDescription {
                foreign_keys,
                primary_key,
                unique_constraints,
                comment,
            } = from_str::<TableDescription>(table_description).map_storage_err()?;

            let schema = Schema {
                table_name: collection_name.to_owned(),
                column_defs,
                indexes: Vec::new(),
                engine: None,
                foreign_keys,
                primary_key,
                unique_constraints,
                comment,
            };

            Ok::<_, Error>(schema)
        });

        Ok(Box::pin(schemas))
    }

    pub async fn get_column_defs(&self, table_name: &str) -> Result<Option<Vec<ColumnDef>>> {
        Ok(self
            .fetch_schema(table_name)
            .await?
            .and_then(|schema| schema.column_defs))
    }
}
