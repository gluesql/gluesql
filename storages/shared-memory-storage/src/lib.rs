mod alter_table;
mod index;
mod metadata;
mod transaction;

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Row, Schema},
        result::{MutResult, Result},
        store::{GStore, GStoreMut, RowIter, Store, StoreMut},
    },
    memory_storage::MemoryStorage,
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
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;

        database.fetch_schema(table_name).await
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Row>> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;

        database.fetch_data(table_name, key).await
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;

        database.scan_data(table_name).await
    }
}

#[async_trait(?Send)]
impl StoreMut for SharedMemoryStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        MemoryStorage::insert_schema(&mut database, schema);

        Ok((self, ()))
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        MemoryStorage::delete_schema(&mut database, table_name);

        Ok((self, ()))
    }

    async fn append_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        MemoryStorage::append_data(&mut database, table_name, rows);

        Ok((self, ()))
    }

    async fn insert_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        MemoryStorage::insert_data(&mut database, table_name, rows);

        Ok((self, ()))
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        MemoryStorage::delete_data(&mut database, table_name, keys);

        Ok((self, ()))
    }
}

impl GStore for SharedMemoryStorage {}
impl GStoreMut for SharedMemoryStorage {}
