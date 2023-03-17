mod alter_table;
mod csv_table;
mod error;
mod index;
mod schema_list;
mod store;
mod store_mut;
mod transaction;

use {
    csv_table::CsvTable,
    error::CsvStorageError,
    std::{collections::HashMap, path::Path},
};

pub(crate) type TableName = String;

pub struct CsvStorage {
    tables: HashMap<TableName, CsvTable>,
}

impl CsvStorage {
    pub fn from_toml(toml_file: impl AsRef<Path>) -> Result<Self, CsvStorageError> {
        Ok(Self {
            tables: HashMap::new(),
        })
    }
}
