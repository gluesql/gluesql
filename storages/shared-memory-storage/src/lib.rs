#![deny(clippy::str_to_string)]

mod alter_table;
mod index;
mod transaction;

use {
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{Error, Result},
        store::{Metadata, Planner, RowIter, Store, StoreMut},
    },
    gluesql_memory_storage::MemoryStorage,
    std::sync::{Arc, RwLock},
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

impl Store for SharedMemoryStorage {
    fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let database = self.database.read().map_err(lock_error)?;

        database.fetch_all_schemas()
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let database = self.database.read().map_err(lock_error)?;

        database.fetch_schema(table_name)
    }

    fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        let database = self.database.read().map_err(lock_error)?;

        database.fetch_data(table_name, key)
    }

    fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let rows = self
            .database
            .read()
            .map_err(lock_error)?
            .scan_data(table_name)
            .into_iter()
            .map(Ok);

        Ok(Box::new(rows))
    }
}

impl StoreMut for SharedMemoryStorage {
    fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let mut database = self.database.write().map_err(lock_error)?;

        database.insert_schema(schema)
    }

    fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let mut database = self.database.write().map_err(lock_error)?;

        database.delete_schema(table_name)
    }

    fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        let mut database = self.database.write().map_err(lock_error)?;

        database.append_data(table_name, rows)
    }

    fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        let mut database = self.database.write().map_err(lock_error)?;

        database.insert_data(table_name, rows)
    }

    fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let mut database = self.database.write().map_err(lock_error)?;

        database.delete_data(table_name, keys)
    }
}

impl Metadata for SharedMemoryStorage {}
impl Planner for SharedMemoryStorage {}
impl gluesql_core::store::CustomFunction for SharedMemoryStorage {}
impl gluesql_core::store::CustomFunctionMut for SharedMemoryStorage {}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> Error {
    Error::StorageMsg("[Shared MemoryStorage] lock poisoned".to_owned())
}
