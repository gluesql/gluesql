use {
    crate::{
        description::{ColumnDescription, TableDescription},
        error::{MongoStorageError, OptionExt, ResultExt},
        row::{key::KeyIntoBson, value::IntoValue, IntoRow},
        utils::get_primary_key,
        MongoStorage,
    },
    async_trait::async_trait,
    futures::{stream, Stream, StreamExt, TryStreamExt},
    gluesql_core::{
        ast::{ColumnDef, ColumnUniqueOption},
        data::{Key, Schema},
        error::Result,
        parse_sql::parse_data_type,
        prelude::{Error, Value},
        store::{DataRow, RowIter, Store},
        translate::translate_data_type,
    },
    mongodb::{
        bson::{doc, document::ValueAccessError, Document},
        options::{FindOptions, ListIndexesOptions},
        IndexModel,
    },
    serde_json::from_str,
    std::{collections::HashMap, future},
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
        let column_defs = self
            .get_column_defs(table_name)
            .await?
            .map_storage_err(MongoStorageError::Unreachable)?;

        let primary_key = get_primary_key(&column_defs)
            .ok_or(MongoStorageError::Unreachable)
            .map_storage_err()?;

        let filter = doc! { "_id": target.to_owned().into_bson(true)?};
        let projection = doc! {"_id": 0};
        let options = FindOptions::builder()
            .projection(projection)
            .sort(doc! { primary_key.name.clone(): 1 })
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
        let column_defs = self.get_column_defs(table_name).await?;

        let primary_key = column_defs
            .as_ref()
            .and_then(|column_defs| get_primary_key(column_defs));

        let has_primary = primary_key.is_some();

        let options = FindOptions::builder();
        let options = match primary_key {
            Some(primary_key) => options.sort(doc! { primary_key.name.to_owned(): 1}).build(),
            None => options.build(),
        };

        let cursor = self
            .db
            .collection::<Document>(table_name)
            .find(Document::new(), options)
            .await
            .map_storage_err()?;

        let column_types = column_defs.as_ref().map(|column_defs| {
            column_defs
                .iter()
                .map(|column_def| column_def.data_type.clone())
                .collect::<Vec<_>>()
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

            let collection = self.db.collection::<Document>(collection_name);
            let options = ListIndexesOptions::builder().build();
            let cursor = collection.list_indexes(options).await.map_storage_err()?;
            let indexes = cursor
                .into_stream()
                .map_err(|e| Error::StorageMsg(e.to_string()))
                .try_filter_map(|index_model| {
                    let IndexModel { keys, options, .. } = index_model;
                    if keys.len() > 1 {
                        return future::ready(Ok::<_, Error>(None));
                    }

                    let index_keys = &mut keys.into_iter().map(|(index_key, _)| index_key);
                    let index_key = index_keys.next();
                    let name = options.and_then(|options| options.name);

                    future::ready(Ok::<_, Error>(index_key.zip(name)))
                })
                .try_collect::<HashMap<String, String>>()
                .await?;

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
                        .map(|x| x == "null")
                        .unwrap_or(false);

                    let data_type = doc
                        .get_str("title")
                        .map_err(|_| MongoStorageError::InvalidGlueType)
                        .map_storage_err()
                        .and_then(parse_data_type)
                        .and_then(|s| translate_data_type(&s))?;

                    let index_name = indexes.get(column_name).and_then(|i| i.split_once('_'));

                    let unique = match index_name {
                        Some((_, "PK")) => Some(true),
                        Some((_, "UNIQUE")) => Some(false),
                        _ => None,
                    }
                    .map(|is_primary| ColumnUniqueOption { is_primary });

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
                        unique,
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
                comment,
            } = from_str::<TableDescription>(table_description).map_storage_err()?;

            let schema = Schema {
                table_name: collection_name.to_owned(),
                column_defs,
                indexes: Vec::new(),
                engine: None,
                foreign_keys,
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
