use std::{
    collections::{HashMap, HashSet},
    future,
    str::FromStr,
};

use futures::{stream, FutureExt, Stream, StreamExt, TryStreamExt};
use gluesql_core::{
    ast::{ColumnDef, ColumnUniqueOption},
    prelude::{DataType, Error},
    store::Index,
};
use mongodb::{
    bson::{doc, Bson, Document},
    options::{IndexOptions, ListIndexesOptions},
    IndexModel,
};

use crate::{
    index,
    value::{BsonType, IntoBson, IntoRow, IntoValue},
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
        println!("fetch_schema: {}", table_name);
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
        println!("fetch_data: {}", table_name);
        let filter = doc! { "_id": target.clone().into_bson()?};
        println!("err1");
        let mut cursor = self
            .db
            .collection::<Document>(table_name)
            .find(filter, None)
            .await
            .map_storage_err()?;

        println!("err2");

        // if column_defs.is_some() => zip values with column_defs

        cursor
            .next()
            .await
            .map(|result| {
                result
                    .map(|doc| {
                        let row = doc
                            .into_iter()
                            .map(|(_, bson)| bson.into_value())
                            .collect::<Vec<_>>();

                        DataRow::Vec(row)
                    })
                    .map_storage_err()
            })
            .transpose()
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        println!("scan_data: {}", table_name);
        println!("err3");
        let cursor = self
            .db
            .collection::<Document>(table_name)
            .find(None, None)
            .await
            .map_storage_err()?;

        let schema = self.fetch_schema(table_name).await?;
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
                    Some(column_types) => {
                        doc.into_row2(column_types.into_iter().map(|data_type| *data_type))
                    }
                    None => doc.into_row(), // TODO: is nested i8, i16 ok?
                }
            })
            .collect::<Vec<_>>()
            .await
            .into_iter();

        // let row_iter = row_iter.collect::<Vec<_>>().await.into_iter(); // TODO: should return Stream

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

        println!("err5,6");
        let validators_list = self
            .db
            .run_command(command, None)
            .await
            .unwrap()
            .get_document("cursor")
            .and_then(|doc| doc.get_array("firstBatch"))
            .unwrap()
            .to_owned();

        let schemas = stream::iter(validators_list)
            // .into_iter()
            .then(move |bson| async move {
                let (collection_name, validators) = bson
                    .as_document()
                    .map(|doc| {
                        let validator = doc
                            .get_document("options")
                            .and_then(|doc| doc.get_document("validator"))
                            .ok();

                        println!("err7-9");
                        let collection_name = doc.get_str("name").unwrap();

                        Ok::<_, Error>((collection_name, validator))
                    })
                    .transpose()
                    .unwrap()
                    .map_storage_err(MongoStorageError::TableDoesNotExist)?;

                println!("validators: {}", validators.unwrap());

                println!("collection_name: {}", collection_name);
                let collection = self.db.collection::<Document>(collection_name);
                let options = ListIndexesOptions::builder().batch_size(10).build();
                let cursor = collection.list_indexes(options).await.unwrap();
                let indexes = cursor
                    .into_stream()
                    .try_filter_map(|index_model| {
                        let index_key = index_model
                            .clone()
                            .keys
                            .into_iter()
                            .map(|(index_key, _)| index_key)
                            .next();

                        let IndexOptions { name, unique, .. } = index_model.options.unwrap();

                        future::ready(Ok(index_key.zip(name)))
                    })
                    .try_collect::<HashMap<String, String>>()
                    .await
                    .unwrap();

                // let indexes = cursor.next().await.unwrap().unwrap();
                // let indexes = cursor.next().await.unwrap().unwrap();
                // .map(|result| {
                //     let index_model = result.unwrap();

                //     index_model.keys
                // })
                // .collect::<Vec<_>>();

                println!("indexes: {:#?}", indexes);

                let column_defs = validators
                    .unwrap()
                    .get_document("$jsonSchema")
                    .unwrap()
                    .get_document("properties")
                    .unwrap()
                    .into_iter()
                    .skip(1)
                    .map(|(column_name, value)| {
                        let data_type = value
                            .as_document()
                            .unwrap()
                            .get_array("bsonType")
                            .unwrap()
                            .get(0)
                            .unwrap()
                            .as_str()
                            .unwrap();

                        let maximum = value.as_document().unwrap().get_i64("maximum").ok();

                        let data_type = BsonType::from_str(data_type).unwrap().into();

                        let data_type = match (data_type, maximum) {
                            (DataType::Int32, Some(B16)) => DataType::Int16,
                            (DataType::Int32, Some(B8)) => DataType::Int8,
                            (DataType::Float, Some(B32)) => DataType::Float32,
                            (data_type, _) => data_type,
                        };

                        let index_name = indexes.get(column_name);
                        let a = index_name.and_then(|i| i.split_once("_"));

                        let unique = match a {
                            Some((_, "PK")) => Some(ColumnUniqueOption { is_primary: true }),
                            Some((_, "UNIQUE")) => Some(ColumnUniqueOption { is_primary: false }),
                            _ => None,
                        };

                        let column_def = ColumnDef {
                            name: column_name.to_owned(),
                            data_type,
                            nullable: true, // should parse from validator
                            default: None,  // does not support default value
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
                    indexes: vec![],
                    engine: None,
                };

                Ok::<_, Error>(schema)
            });

        // println!("fetch_schemas_iter done: {:?}", schemas);
        Ok(Box::pin(schemas))
    }
}

pub const B16: i64 = 2_i64.pow(16);
pub const B8: i64 = 2_i64.pow(8);
pub const B32: i64 = 2_i64.pow(32);
