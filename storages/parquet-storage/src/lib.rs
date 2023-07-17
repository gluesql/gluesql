use error::{OptionExt, ResultExt};
mod alter_table;
mod error;
mod function;
mod index;
mod store;
mod store_mut;
mod transaction;

use {
    error::ParquetStorageError,
    gluesql_core::{
        ast::ColumnUniqueOption,
        data::{value::HashMapJsonExt, Schema},
        error::{Error, Result},
        prelude::Key,
        store::{DataRow, Metadata, RowIter},
    },
    serde_json::Value as JsonValue,
    std::{
        collections::HashMap,
        fs::{self, File},
        io::Read,
        path::PathBuf,
    },
};

#[derive(Debug)]
pub struct ParquetStorage {
    pub path: PathBuf,
}

impl ParquetStorage {
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let path = PathBuf::from(path);
        Ok(Self { path })
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        if !self.data_path(table_name).exists() {
            return Ok(None);
        };

        let schema_path = self.schema_path(table_name);
        let column_defs = match schema_path.exists() {
            true => {
                let mut file = File::open(&schema_path).map_storage_err()?;
                let mut ddl = String::new();
                file.read_to_string(&mut ddl).map_storage_err()?;

                let schema = Schema::from_ddl(&ddl)?;
                if schema.table_name != table_name {
                    return Err(Error::StorageMsg(
                        ParquetStorageError::TableNameDoesNotMatchWithFile.to_string(),
                    ));
                }

                schema.column_defs
            }
            false => None,
        };

        Ok(Some(Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes: vec![],
            engine: None,
        }))
    }

    fn data_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "parquet")
    }

    fn schema_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "sql")
    }

    fn path_by(&self, table_name: &str, extension: &str) -> PathBuf {
        let path = self.path.as_path();
        let mut path = path.join(table_name);
        path.set_extension(extension);

        path
    }

    fn scan_data(&self, table_name: &str) -> Result<(RowIter, Schema)> {
        let schema = self
            .fetch_schema(table_name)?
            .map_storage_err(ParquetStorageError::TableDoesNotExist)?;
        todo!();
        // let rows = RowIter();

        // Ok((Box::new(rows), schema))
    }
}

impl Metadata for ParquetStorage {}
