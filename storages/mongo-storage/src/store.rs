use gluesql_core::{
    ast::{ColumnDef, ColumnUniqueOption},
    prelude::{DataType, Error},
};
use mongodb::{
    bson::{doc, Document},
    options::FindOneOptions,
};

use {
    crate::{
        error::{JsonStorageError, OptionExt, ResultExt},
        MongoStorage,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, RowIter, Store},
    },
    std::{ffi::OsStr, fs},
};

#[async_trait(?Send)]
impl Store for MongoStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let command = doc! { "listCollections": 1, "filter": { "name": table_name } };
        let collection_info = self.db.run_command(command, None).await.map_storage_err()?;
        let validators = collection_info
            .get_document("cursor")
            .and_then(|doc| doc.get_array("firstBatch"))
            .map_storage_err()?
            .get(0)
            .and_then(|bson| bson.as_document())
            .map(|doc| {
                doc.get_document("options")
                    .and_then(|doc| doc.get_document("options"))
                    .and_then(|doc| doc.get_document("validator"))
            })
            .transpose()
            .map_storage_err()?;

        let column_defs = validators
            .map(|v| {
                v.into_iter()
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
            table_name: table_name.to_string(),
            column_defs,
            indexes: vec![],
            engine: None,
        };

        Ok(Some(schema))
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        // let

        // schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        // Ok(schemas)
        todo!();
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        for item in self.scan_data(table_name)?.0 {
            let (key, row) = item?;

            if &key == target {
                return Ok(Some(row));
            }
        }

        Ok(None)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        Ok(self.scan_data(table_name)?.0)
    }
}
