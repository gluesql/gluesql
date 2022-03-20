mod alter_table;
mod index;
mod metadata;
mod transaction;

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{Row, Schema},
        result::{MutResult, Result},
        store::{GStore, GStoreMut, RowIter, Store, StoreMut},
    },
    std::{
        collections::{BTreeMap, HashMap},
        iter::empty,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    },
    tokio::sync::RwLock,
};

#[derive(Debug)]
pub struct Key {
    pub table_name: String,
    pub id: u64,
}

#[derive(Debug)]
pub struct Item {
    pub schema: Schema,
    pub rows: BTreeMap<u64, Row>,
}

#[derive(Debug)]
pub struct MemoryStorage {
    pub id_counter: AtomicU64,
    pub items: Arc<RwLock<HashMap<String, Item>>>,
}

#[async_trait(?Send)]
impl Store<Key> for MemoryStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let items = Arc::clone(&self.items);
        let schema = items
            .read()
            .await
            .get(table_name)
            .map(|item| Ok(item.schema.clone()))
            .transpose();

        schema
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<Key>> {
        let items = Arc::clone(&self.items);
        let items = items.read().await;

        let rows = match items.get(table_name) {
            Some(item) => &item.rows,
            None => return Ok(Box::new(empty())),
        };

        let rows = rows
            .iter()
            .map(|(id, row)| {
                let key = Key {
                    table_name: table_name.to_owned(),
                    id: *id,
                };

                Ok((key, row.clone()))
            })
            .collect::<Vec<_>>()
            .into_iter();

        Ok(Box::new(rows))
    }
}

#[async_trait(?Send)]
impl StoreMut<Key> for MemoryStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let storage = self;

        let table_name = schema.table_name.clone();
        let item = Item {
            schema: schema.clone(),
            rows: BTreeMap::new(),
        };

        storage.items.write().await.insert(table_name, item);
        Ok((storage, ()))
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let storage = self;

        storage.items.write().await.remove(table_name);
        Ok((storage, ()))
    }

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        let storage = self;

        if let Some(item) = storage.items.write().await.get_mut(table_name) {
            for row in rows {
                let id = storage.id_counter.fetch_add(1, Ordering::SeqCst);
                item.rows.insert(id, row);
            }
        }

        Ok((storage, ()))
    }

    async fn update_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        let storage = self;
        let items = Arc::clone(&storage.items);
        let mut items = items.write().await;

        if let Some(item) = items.get_mut(table_name) {
            for (key, row) in rows {
                let id = key.id;

                item.rows.insert(id, row);
            }
        }

        Ok((storage, ()))
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let storage = self;
        let items = Arc::clone(&storage.items);
        let mut items = items.write().await;

        if let Some(item) = items.get_mut(table_name) {
            for key in keys {
                item.rows.remove(&key.id);
            }
        }

        Ok((storage, ()))
    }
}

impl GStore<Key> for MemoryStorage {}
impl GStoreMut<Key> for MemoryStorage {}
