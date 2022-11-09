use std::collections::HashMap;

use csv_table::CsvTable;

mod csv_table;
mod error;
mod schema_list;
mod store;

use {error::StorageError, std::path::Path};

pub(crate) type TableName = String;

pub struct CsvStorage {
    tables: HashMap<TableName, CsvTable>,
}

impl CsvStorage {
    pub fn from_toml(toml_file: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self {
            tables: HashMap::new(),
        })
    }
}
