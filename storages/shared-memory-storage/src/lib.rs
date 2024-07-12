#![deny(clippy::str_to_string)]

mod alter_table;
mod index;
mod transaction;

use {
    async_trait::async_trait,
    futures::stream,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, Metadata, RowIter, Store, StoreMut},
    },
    gluesql_memory_storage::MemoryStorage,
    std::sync::Arc,
    tokio::sync::RwLock,
};

#[derive(Clone, Debug)]
pub struct SharedMemoryStorage {
    pub database: Arc<RwLock<MemoryStorage>>,
}

impl SharedMemoryStorage {
    pub fn new() -> Self {
        let database = MemoryStorage::default();
        let database = Arc::new(RwLock::new(database));

        Self { database }
    }
}

impl Default for SharedMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl From<MemoryStorage> for SharedMemoryStorage {
    fn from(storage: MemoryStorage) -> Self {
        let database = Arc::new(RwLock::new(storage));
        Self { database }
    }
}

#[async_trait(?Send)]
impl Store for SharedMemoryStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;

        database.fetch_all_schemas().await
    }
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;

        database.fetch_schema(table_name).await
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;

        database.fetch_data(table_name, key).await
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let rows = self
            .database
            .read()
            .await
            .scan_data(table_name)
            .into_iter()
            .map(Ok);

        Ok(Box::pin(stream::iter(rows)))
    }
}

#[async_trait(?Send)]
impl StoreMut for SharedMemoryStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        database.insert_schema(schema).await
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        database.delete_schema(table_name).await
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        database.append_data(table_name, rows).await
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        database.insert_data(table_name, rows).await
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        database.delete_data(table_name, keys).await
    }
}

impl Metadata for SharedMemoryStorage {}
impl gluesql_core::store::CustomFunction for SharedMemoryStorage {}
impl gluesql_core::store::CustomFunctionMut for SharedMemoryStorage {}
