use async_trait::async_trait;
use gluesql_core::prelude::Key;
use gluesql_core::{
    data::Schema,
    error::{Error, Result},
    store::{DataRow, RowIter, Store, StoreMut},
};
use redb::Database;

pub struct RedbStorage {
    namespace: String,
    database: Database,
}

#[async_trait(?Send)]
impl Store for RedbStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        todo!()
    }
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        todo!()
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        todo!()
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        todo!()
    }
}

#[async_trait(?Send)]
impl StoreMut for RedbStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        todo!()
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        todo!()
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        todo!()
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        todo!()
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        todo!()
    }
}
