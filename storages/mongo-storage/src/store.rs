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
    utils::get_primary_key,
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

        let column_defs = self
            .fetch_schema(table_name)
            .await?
            .and_then(|schema| schema.column_defs);

        let primary_key = column_defs.clone().map(get_primary_key).flatten();

        let filter = match primary_key.clone() {
            Some(primary_key) => {
                doc! { primary_key.name: target.clone().into_bson()?}
            }
            None => doc! { "_id": target.clone().into_bson()? },
        };

        // let filter = doc! { "_id": target.clone().into_bson()?};
        println!("err1");

        // let projection = column_defs.map(|x| {
        //     x.into_iter().fold(doc! {"_id": 0}, |mut acc, cur| {
        //         acc.extend(doc! {cur.name: 1});

        //         acc
        //     })
        // });
        let projection = doc! {"_id": 0};

        let options = FindOptions::builder().projection(projection);
        let options = match primary_key.clone() {
            Some(primary_key) => options.sort(doc! { primary_key.name: 1}).build(),
            None => options.build(),
        };

        let mut cursor = self
            .db
            .collection::<Document>(table_name)
            .find(filter, options)
            .await
            .unwrap();
        // .map_storage_err()?;

        // if column_defs.is_some() => zip values with column_defs

        Ok(cursor
            .next()
            .await
            .map(|result| {
                result.map(|doc| {
                    let row = doc
                        .into_iter()
                        .map(|(_, bson)| bson.into_value())
                        .collect::<Vec<_>>();

                    DataRow::Vec(row)
                })
                // .map_storage_err()
            })
            .transpose()
            .unwrap())
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        println!("scan_data: {}", table_name);
        println!("err3");
        let schema = self.fetch_schema(table_name).await?;
        let column_defs = self
            .fetch_schema(table_name)
            .await?
            .and_then(|schema| schema.column_defs);

        let primary_key = column_defs.clone().map(get_primary_key).flatten();
        // let projection = doc! {"_id": 0}; // TODO: optional

        let options = FindOptions::builder(); //.projection(projection);
        let options = match primary_key.clone() {
            Some(primary_key) => options.sort(doc! { primary_key.name: 1}).build(),
            None => options.build(),
        };

        let cursor = self
            .db
            .collection::<Document>(table_name)
            .find(doc! {}, options)
            .await
            .unwrap();
        // .map_storage_err()?;

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
                let doc = doc.unwrap();

                match &column_types {
                    Some(column_types) => {
                        doc.into_row2(column_types.into_iter().map(|data_type| *data_type))
                    }
                    None => {
                        // let key = doc.get_object_id("_id").unwrap();
                        let mut iter = doc.into_iter();
                        let (_, value) = iter.next().unwrap();
                        let key_bytes = value.as_object_id().unwrap().bytes().to_vec();
                        let key = Key::Bytea(key_bytes);
                        let row = iter
                            .map(|(key, bson)| (key, bson.into_value()))
                            .collect::<HashMap<String, Value>>();

                        Ok((key, DataRow::Map(row)))
                    }
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
                    .unwrap();
                // .map_storage_err(MongoStorageError::TableDoesNotExist)?;

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
                        let mut iter = value
                            .as_document()
                            .unwrap()
                            // .map_storage_err(MongoStorageError::InvalidDocument)?
                            .get_array("bsonType")
                            .unwrap()
                            // .map_storage_err()?
                            // .get(0..1)
                            // .map_storage_err(MongoStorageError::InvalidDocument)?
                            .iter()
                            .map(|x| {
                                x.as_str()// .unwrap()
                                .map_storage_err(MongoStorageError::InvalidDocument)
                            });

                        // let maximum = value.as_document().unwrap().get_i64("maximum").ok();
                        let type_str = value.as_document().unwrap().get_str("title").unwrap();
                        let a = parse_data_type(type_str).unwrap();
                        let data_type = translate_data_type(&a).unwrap();

                        // let data_type = BsonType::from_str(iter.next().unwrap().unwrap())
                        //     .unwrap()
                        //     .into();

                        let nullable = iter
                            .skip(1)
                            .next()
                            .transpose()?
                            .map(|x| x == "null")
                            .unwrap_or(false);

                        // TODO: remove indent
                        // let data_type = match (data_type, maximum) {
                        //     (DataType::Int32, Some(B8)) => DataType::Int8,
                        //     (DataType::Int32, Some(B16)) => DataType::Int16,
                        //     (DataType::Float, Some(B32)) => DataType::Float32,
                        //     (DataType::Date, Some(TIME)) => DataType::Time,
                        //     // (DataType::Int32, Some(bson))
                        //     //     if bson.as_i64().filter(|x| x == &B16).is_some() =>
                        //     // {
                        //     //     DataType::Int16
                        //     // }
                        //     // (DataType::Int32, Some(bson))
                        //     //     if bson.as_i64().filter(|x| x == &B8).is_some() =>
                        //     // {
                        //     //     DataType::Int8
                        //     // }
                        //     // (DataType::Float, Some(bson))
                        //     //     if bson.as_i64().filter(|x| x == &B32).is_some() =>
                        //     // {
                        //     //     DataType::Float32
                        //     // }
                        //     (data_type, _) => data_type,
                        // };

                        let index_name = indexes.get(column_name);
                        let a = index_name.and_then(|i| i.split_once("_"));

                        let unique = match a {
                            Some((_, "PK")) => Some(ColumnUniqueOption { is_primary: true }),
                            Some((_, "UNIQUE")) => Some(ColumnUniqueOption { is_primary: false }),
                            _ => None,
                        };

                        let default = value
                            .as_document()
                            .unwrap()
                            .get_str("description")
                            .ok()
                            .map(|str| {
                                let expr = parse_expr(str).unwrap();

                                translate_expr(&expr)
                            })
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
                    indexes: vec![],
                    engine: None,
                };

                Ok::<_, Error>(schema)
            });

        println!("fetch_schemas_iter done: {:#?}", stringify!(schemas));
        Ok(Box::pin(schemas))
    }
}

pub const B16: i64 = 2_i64.pow(16);
pub const B8: i64 = 2_i64.pow(8);
pub const B32: i64 = 2_i64.pow(32);
pub const TIME: i64 = 86400000 - 1;
