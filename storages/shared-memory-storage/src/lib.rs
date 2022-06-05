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
    indexmap::IndexMap,
    std::{
        collections::HashMap,
        iter::empty,
        sync::{
            atomic::{AtomicI64, Ordering},
            Arc,
        },
    },
    tokio::sync::RwLock,
};

#[derive(Debug)]
pub struct Item {
    pub schema: Schema,
    pub rows: IndexMap<Key, Row>,
}

#[derive(Debug)]
pub struct SharedMemoryStorage {
    pub database: Arc<MemoryStorage>,
}

impl Clone for SharedMemoryStorage {
    fn clone(&self) -> Self {
        Self {
            database: Arc::clone(&self.database),
        }
    }
}

#[derive(Debug)]
pub struct MemoryStorage {
    pub id_counter: AtomicI64,
    pub items: Arc<RwLock<HashMap<String, Item>>>,
}

impl SharedMemoryStorage {
    pub fn new() -> Self {
        let database = MemoryStorage {
            id_counter: AtomicI64::new(0),
            items: Arc::new(RwLock::new(HashMap::new())),
        };
        let database = Arc::new(database);

        Self { database }
    }
}

impl Default for SharedMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl Store for SharedMemoryStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let database = Arc::clone(&self.database);

        let schema = database
            .items
            .read()
            .await
            .get(table_name)
            .map(|item| Ok(item.schema.clone()))
            .transpose();

        schema
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let database = Arc::clone(&self.database);
        let items = database.items.read().await;

        let rows: RowIter = match items.get(table_name) {
            Some(item) => Box::new(item.rows.clone().into_iter().map(Ok)),
            None => Box::new(empty()),
        };

        Ok(rows)
    }
}

#[async_trait(?Send)]
impl StoreMut for SharedMemoryStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);

        let table_name = schema.table_name.clone();
        let item = Item {
            schema: schema.clone(),
            rows: IndexMap::new(),
        };

        database.items.write().await.insert(table_name, item);
        Ok((self, ()))
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);

        database.items.write().await.remove(table_name);
        Ok((self, ()))
    }

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);

        if let Some(item) = database.items.write().await.get_mut(table_name) {
            for row in rows {
                let id = database.id_counter.fetch_add(1, Ordering::SeqCst);

                item.rows.insert(Key::I64(id), row);
            }
        }

        Ok((self, ()))
    }

    async fn update_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut items = database.items.write().await;

        if let Some(item) = items.get_mut(table_name) {
            for (key, row) in rows {
                item.rows.insert(key, row);
            }
        }

        Ok((self, ()))
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut items = database.items.write().await;

        if let Some(item) = items.get_mut(table_name) {
            for key in keys {
                item.rows.remove(&key);
            }
        }

        Ok((self, ()))
    }
}

impl GStore for SharedMemoryStorage {}
impl GStoreMut for SharedMemoryStorage {}
