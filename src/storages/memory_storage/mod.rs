mod alter_table;
mod index;
mod transaction;

use {
    crate::{
        data::{Row, Schema},
        result::{MutResult, Result},
        store::{GStore, GStoreMut, RowIter, Store, StoreMut},
    },
    async_trait::async_trait,
    std::{
        collections::{BTreeMap, HashMap},
        iter::empty,
    },
};

#[derive(Debug, Clone)]
pub struct Key {
    pub table_name: String,
    pub id: u64,
}

#[derive(Debug, Clone)]
struct Item {
    schema: Schema,
    rows: BTreeMap<u64, Row>,
}

#[derive(Debug, Default, Clone)]
pub struct MemoryStorage {
    id_counter: u64,
    items: HashMap<String, Item>,
}

#[async_trait(?Send)]
impl Store<Key> for MemoryStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.items
            .get(table_name)
            .map(|item| item.schema.clone())
            .map(Ok)
            .transpose()
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<Key>> {
        let rows = match self.items.get(table_name) {
            Some(item) => &item.rows,
            None => {
                return Ok(Box::new(empty()));
            }
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
        let mut storage = self;

        let table_name = schema.table_name.clone();
        let item = Item {
            schema: schema.clone(),
            rows: BTreeMap::new(),
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

                item.rows.insert(id, row);
            }
        }

        storage.id_counter = id;
        Ok((storage, ()))
    }

    async fn update_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        let mut storage = self;

        if let Some(item) = storage.items.get_mut(table_name) {
            for (key, row) in rows {
                let id = key.id;

                item.rows.insert(id, row);
            }
        }

        Ok((storage, ()))
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let mut storage = self;

        if let Some(item) = storage.items.get_mut(table_name) {
            for key in keys {
                item.rows.remove(&key.id);
            }
        }

        Ok((storage, ()))
    }
}

impl GStore<Key> for MemoryStorage {}
impl GStoreMut<Key> for MemoryStorage {}
