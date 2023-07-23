use futures::StreamExt;
use gluesql_core::{
    ast::ColumnDef,
    prelude::{DataType, Error},
};
use mongodb::bson::{doc, Document};

use crate::value::{IntoBson, IntoRow, IntoValue};

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
            .transpose()
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        self.fetch_schemas_iter(None)
            .await?
            .collect::<Result<Vec<_>>>()
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

        let row_iter = cursor.map(|doc| {
            println!("err4");
            let doc = doc.map_storage_err()?;

            doc.into_row()
        });

        let row_iter = row_iter.collect::<Vec<_>>().await.into_iter();

        Ok(Box::new(row_iter))
    }
}

impl MongoStorage {
    async fn fetch_schemas_iter(
        &self,
        table_name: Option<&str>,
    ) -> Result<impl Iterator<Item = Result<Schema>>> {
        let command = match table_name {
            Some(table_name) => doc! { "listCollections": 1, "filter": { "name": table_name } },
            None => doc! { "listCollections": 1 },
        };

        println!("err5,6");
        let validators_list = self
            .db
            .run_command(command, None)
            .await
            .map_storage_err()?
            .get_document("cursor")
            .and_then(|doc| doc.get_array("firstBatch"))
            .map_storage_err()?
            .to_owned();

        let schemas = validators_list.into_iter().map(|bson| {
            let (collection_name, validators) = bson
                .as_document()
                .map(|doc| {
                    let validator = doc
                        .get_document("options")
                        .and_then(|doc| doc.get_document("validator"))
                        .ok();

                    println!("err7-9");
                    let collection_name = doc.get_str("name").map_storage_err()?;

                    Ok::<_, Error>((collection_name, validator))
                })
                .transpose()
                .map_storage_err()?
                .map_storage_err(MongoStorageError::TableDoesNotExist)?;

            println!("validators: {}", validators.unwrap());
            let column_defs = validators
                .unwrap()
                .get_document("$jsonSchema")
                .unwrap()
                .get_document("properties")
                .unwrap()
                .into_iter()
                .skip(1)
                .map(|(name, value)| {
                    let data_type = value
                        .as_document()
                        .unwrap()
                        .get_array("bsonType")
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .as_str()
                        .unwrap();

                    let data_type = match data_type {
                        "string" => DataType::Text,
                        "int" => DataType::Int,
                        "object" => DataType::Map,
                        "array" => DataType::List,
                        "long" => DataType::Int,
                        v => {
                            println!("v: {}", v);
                            todo!();
                        }
                    };

                    let column_def = ColumnDef {
                        name: name.to_owned(),
                        data_type,
                        nullable: true, // should parse from validator
                        default: None,  // does not support default value
                        unique: None,   // by unique index?
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

            Ok(schema)
        });

        println!("fetch_schemas_iter done: {:?}", schemas);
        Ok(schemas)
    }
}
