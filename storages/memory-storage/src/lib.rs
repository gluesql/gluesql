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
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, iter::empty},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub schema: Schema,
    pub rows: IndexMap<Key, Row>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MemoryStorage {
    pub id_counter: i64,
    pub items: HashMap<String, Item>,
}

#[async_trait(?Send)]
impl Store<Key> for MemoryStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.items
            .get(table_name)
            .map(|item| Ok(item.schema.clone()))
            .transpose()
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<Key>> {
        let rows: RowIter<_> = match self.items.get(table_name) {
            Some(item) => Box::new(item.rows.clone().into_iter().map(Ok)),
            None => Box::new(empty()),
        };

        Ok(rows)
    }
}

#[async_trait(?Send)]
impl StoreMut<Key> for MemoryStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let mut storage = self;

        let table_name = schema.table_name.clone();
        let item = Item {
            schema: schema.clone(),
            rows: IndexMap::new(),
        };

        storage.items.insert(table_name, item);
        Ok((storage, ()))
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let mut storage = self;

        storage.items.remove(table_name);
        Ok((storage, ()))
    }

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        let mut storage = self;
        let mut id = storage.id_counter;

        if let Some(item) = storage.items.get_mut(table_name) {
            for row in rows {
                id += 1;

                item.rows.insert(Key::I64(id), row);
            }
        }

        storage.id_counter = id;
        Ok((storage, ()))
    }

    async fn update_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        let mut storage = self;

        if let Some(item) = storage.items.get_mut(table_name) {
            for (key, row) in rows {
                item.rows.insert(key, row);
            }
        }

        Ok((storage, ()))
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let mut storage = self;

        if let Some(item) = storage.items.get_mut(table_name) {
            for key in keys {
                item.rows.remove(&key);
            }
        }

        Ok((storage, ()))
    }
}

impl GStore<Key> for MemoryStorage {}
impl GStoreMut<Key> for MemoryStorage {}
