use std::path::Path;

use error::StorageError;
use gluesql_core::data::Schema;
use schema_list::get_schema_list;

mod csv_table;
mod error;
mod schema_list;
mod store;

pub struct CsvStorage {
    schema_list: Vec<Schema>,
}

impl CsvStorage {
    pub fn from_toml(toml_file: impl AsRef<Path>) -> Result<Self, StorageError> {
        let schema_list = get_schema_list(toml_file)?;
        Ok(Self { schema_list })
    }
}
