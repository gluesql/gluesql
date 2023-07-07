use futures::{StreamExt, TryStreamExt};
use gluesql_core::{
    ast::ColumnDef,
    prelude::{DataType, Error, Value},
};
use mongodb::bson::{doc, Bson, Document};

use crate::value::{IntoRow, IntoValue};

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
            .transpose()
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        self.fetch_schemas_iter(None)
            .await?
            .collect::<Result<Vec<_>>>()
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        let filter = match target {
            Key::U8(key) => doc! { "_id": key.into() },
            Key::Str(key) => doc! { "_id": key },
            _ => todo!(),
        };

        let cursor = self
            .db
            .collection::<Document>(table_name)
            .find(filter, None)
            .await
            .map_storage_err()?;

        let a = cursor.next().await.map(|a| {
            a.map(|doc| {
                doc.into_iter()
                    .map(|(_, bson)| bson.into_value())
                    .collect::<Vec<_>>()
            })
        });
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let cursor = self
            .db
            .collection::<Document>(table_name)
            .find(None, None)
            .await
            .map_storage_err()?;

        let row_iter = cursor.map(|doc| {
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
                        .and_then(|doc| doc.get_document("options"))
                        .and_then(|doc| doc.get_document("validator"))
                        .ok();

                    let collection_name = doc.get_str("name").map_storage_err()?;

                    Ok::<_, Error>((collection_name, validator))
                })
                .transpose()
                .map_storage_err()?
                .map_storage_err(MongoStorageError::TableDoesNotExist)?;

            let column_defs = validators
                .map(|validators| {
                    validators
                        .into_iter()
                        .map(|(key, value)| {
                            let data_type = value
                                .as_document()
                                .unwrap()
                                .get_str("$type")
                                .map_storage_err()?;

                            let data_type = match data_type {
                                "string" => DataType::Text,
                                "number" => DataType::Int,
                                "object" => DataType::Map,
                                "array" => DataType::List,
                                _ => todo!(),
                            };

                            let column_def = ColumnDef {
                                name: key.to_string(),
                                data_type,
                                nullable: true, // should parse from validator
                                default: None,  // does not support default value
                                unique: None,   // bby unique index?
                            };

                            Ok(column_def)
                        })
                        .collect::<Result<Vec<ColumnDef>>>()
                })
                .transpose()?;

            let schema = Schema {
                table_name: collection_name.to_owned(),
                column_defs,
                indexes: vec![],
                engine: None,
            };

            Ok(schema)
        });

        Ok(schemas)
    }
}
