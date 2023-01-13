#![deny(clippy::str_to_string)]

mod alter_table;
mod index;
mod transaction;

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        result::{MutResult, Result},
        store::{DataRow, RowIter, Store, StoreMut},
    },
    indexmap::IndexMap,
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, iter::empty},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub schema: Schema,
    pub rows: IndexMap<Key, DataRow>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MemoryStorage {
    pub id_counter: i64,
    pub items: HashMap<String, Item>,
}

#[async_trait(?Send)]
impl Store for MemoryStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut schemas = self
            .items
            .values()
            .map(|item| item.schema.clone())
            .collect::<Vec<_>>();
        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.items
            .get(table_name)
            .map(|item| Ok(item.schema.clone()))
            .transpose()
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        let row = self
            .items
            .get(table_name)
            .and_then(|item| item.rows.get(key).map(Clone::clone));

        Ok(row)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let rows: RowIter = match self.items.get(table_name) {
            Some(item) => Box::new(item.rows.clone().into_iter().map(Ok)),
            None => Box::new(empty()),
        };

        Ok(rows)
    }
}

impl MemoryStorage {
    pub fn insert_schema(&mut self, schema: &Schema) {
        let table_name = schema.table_name.clone();
        let item = Item {
            schema: schema.clone(),
            rows: IndexMap::new(),
        };

        self.items.insert(table_name, item);
    }

    pub fn delete_schema(&mut self, table_name: &str) {
        self.items.remove(table_name);
    }

    pub fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) {
        if let Some(item) = self.items.get_mut(table_name) {
            for row in rows {
                self.id_counter += 1;

                item.rows.insert(Key::I64(self.id_counter), row);
            }
        }
    }

    pub fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) {
        if let Some(item) = self.items.get_mut(table_name) {
            for (key, row) in rows {
                item.rows.insert(key, row);
            }
        }
    }

    pub fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) {
        if let Some(item) = self.items.get_mut(table_name) {
            for key in keys {
                item.rows.remove(&key);
            }
        }
    }
}

#[async_trait(?Send)]
impl StoreMut for MemoryStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let mut storage = self;

        MemoryStorage::insert_schema(&mut storage, schema);

        Ok((storage, ()))
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let mut storage = self;

        MemoryStorage::delete_schema(&mut storage, table_name);

        Ok((storage, ()))
    }

    async fn append_data(self, table_name: &str, rows: Vec<DataRow>) -> MutResult<Self, ()> {
        let mut storage = self;

        MemoryStorage::append_data(&mut storage, table_name, rows);

        Ok((storage, ()))
    }

    async fn insert_data(self, table_name: &str, rows: Vec<(Key, DataRow)>) -> MutResult<Self, ()> {
        let mut storage = self;

        MemoryStorage::insert_data(&mut storage, table_name, rows);

        Ok((storage, ()))
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let mut storage = self;

        MemoryStorage::delete_data(&mut storage, table_name, keys);

        Ok((storage, ()))
    }
}
