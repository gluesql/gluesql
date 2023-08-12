use std::{
    collections::{HashMap, HashSet},
    future,
    str::FromStr,
};

use futures::{stream, FutureExt, Stream, StreamExt, TryStreamExt};
use gluesql_core::{
    ast::{ColumnDef, ColumnUniqueOption, Expr},
    parse_sql::{parse_data_type, parse_expr},
    prelude::{DataType, Error, Value},
    store::Index,
    translate::{translate_data_type, translate_expr},
};
use mongodb::{
    bson::{doc, Bson, Document},
    options::{FindOptions, IndexOptions, ListIndexesOptions},
    IndexModel,
};

use crate::{
    index,
    row::{key::KeyIntoBson, value::IntoValue, IntoRow},
    utils::get_primary_key,
};

use {
    crate::{
        error::{MongoStorageError, OptionExt, ResultExt},
        MongoStorage,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, RowIter, Store},
    },
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

        let primary_key = get_primary_key(&column_defs);
        let filter = doc! { "_id": target.to_owned().into_bson(primary_key.is_some())?};
        let projection = doc! {"_id": 0};
        let options = FindOptions::builder().projection(projection);
        let options = match primary_key {
            Some(primary_key) => options.sort(doc! { primary_key.name.clone(): 1 }).build(),
            None => options.build(),
        };

        let mut cursor = self
            .db
            .collection::<Document>(table_name)
            .find(filter, options)
            .await
            .map_storage_err()?;

        Ok(cursor
            .next()
            .await
            .transpose()
            .map_storage_err()?
            .map(|doc| {
                let row = doc
                    .into_iter()
                    .zip(column_defs.iter())
                    .map(|((_, bson), column_def)| bson.into_value(&column_def.data_type))
                    .collect::<Vec<_>>();

                DataRow::Vec(row)
            }))
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let schema = self.fetch_schema(table_name).await?;
        let column_defs = self.get_column_defs(table_name).await?;

        let primary_key = column_defs.as_ref().and_then(get_primary_key);

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

        let column_types = schema.as_ref().and_then(|schema| {
            schema.column_defs.as_ref().map(|column_defs| {
                column_defs
                    .iter()
                    .map(|column_def| &column_def.data_type)
                    .collect::<Vec<_>>()
            })
        });

        let row_iter = cursor
            .map(|doc| {
                let doc = doc.map_storage_err()?;

                match &column_types {
                    Some(column_types) => doc.into_row(
                        column_types.into_iter().map(|data_type| *data_type),
                        primary_key.is_some(),
                    ),
                    None => {
                        let mut iter = doc.into_iter();
                        let (_, value) = iter.next().unwrap();
                        let key_bytes = value.as_object_id().unwrap().bytes().to_vec();
                        let key = Key::Bytea(key_bytes);
                        let row = iter
                            .map(|(key, bson)| (key, bson.into_value_schemaless()))
                            .collect::<HashMap<String, Value>>();

                        Ok((key, DataRow::Map(row)))
                    }
                }
            })
            .collect::<Vec<_>>()
            .await
            .into_iter();

        Ok(Box::new(row_iter))
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
            let validators = doc
                .get_document("options")
                .and_then(|doc| doc.get_document("validator"))
                .map_storage_err()?;

            let collection = self.db.collection::<Document>(collection_name);
            let options = ListIndexesOptions::builder().build();
            let cursor = collection.list_indexes(options).await.unwrap();
            let indexes = cursor
                .into_stream()
                .try_filter_map(|index_model| {
                    let IndexModel { keys, options, .. } = index_model;
                    let index_key = keys.into_iter().map(|(index_key, _)| index_key).next(); // TODO: should throw error if there are multiple keys?
                    let name = options.and_then(|options| options.name);

                    future::ready(Ok(index_key.zip(name)))
                })
                .try_collect::<HashMap<String, String>>()
                .await
                .map_storage_err()?;

            let column_defs = validators
                .get_document("$jsonSchema")
                .and_then(|doc| doc.get_document("properties"))
                .map_storage_err()?
                .into_iter()
                .skip(1)
                .map(|(column_name, doc)| {
                    let doc = doc
                        .as_document()
                        .map_storage_err(MongoStorageError::InvalidDocument)?;

                    let nullable = doc
                        .get_array("bsonType")
                        .map_storage_err()?
                        .get(1)
                        .and_then(|x| x.as_str())
                        .map(|x| x == "null")
                        .unwrap_or(false);

                    let data_type = doc
                        .get_str("title")
                        .map_storage_err()
                        .and_then(parse_data_type)
                        .and_then(|s| translate_data_type(&s))?;

                    let index_name = indexes.get(column_name).and_then(|i| i.split_once("_"));

                    let unique = match index_name {
                        Some((_, "PK")) => Some(true),
                        Some((_, "UNIQUE")) => Some(false),
                        _ => None,
                    }
                    .map(|is_primary| ColumnUniqueOption { is_primary });

                    let default = doc
                        .get_str("description")
                        .ok()
                        .map(parse_expr)
                        .map(|expr| expr.and_then(|expr| translate_expr(&expr)))
                        .transpose()?;

                    let column_def = ColumnDef {
                        name: column_name.to_owned(),
                        data_type,
                        nullable,
                        default,
                        unique,
                    };

                    Ok(column_def)
                })
                .collect::<Result<Vec<ColumnDef>>>()?;

            let column_defs = match column_defs.len() {
                0 => None,
                _ => Some(column_defs),
            };

            let schema = Schema {
                table_name: collection_name.to_owned(),
                column_defs,
                indexes: Vec::new(),
                engine: None,
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
