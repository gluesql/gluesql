use {
    crate::{
        error::{OptionExt, ParquetStorageError, ResultExt},
        ParquetStorage,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, RowIter, Store},
    },
    serde_json::Value as JsonValue,
    std::{ffi::OsStr, fs},
};

#[async_trait(?Send)]
#[deny(implied_bounds_entailment)]
impl Store for ParquetStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.fetch_schema(table_name)
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        todo!();
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        todo!();
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        Ok(self.scan_data(table_name)?.0)
    }
}
