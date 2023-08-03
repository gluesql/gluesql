#![deny(clippy::str_to_string)]

mod alter_table;
mod error;
mod index;
mod metadata;
mod snapshot;
mod stage;
mod transaction;

use {
    crate::snapshot::Snapshot,
    async_trait::async_trait,
    gluesql_core::{
        chrono::Utc,
        data::{CustomFunction as StructCustomFunction, Key, Schema, Value},
        error::Result,
        store::{CustomFunction, CustomFunctionMut, DataRow, RowIter, Store, StoreMut},
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::{BTreeMap, HashMap},
        iter::empty,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageState {
    Idle,
    Transaction { autocommit: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub schema: Schema,
    pub rows: BTreeMap<Key, DataRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStorage {
    pub id_counter: i64,
    pub items: HashMap<String, Item>,
    pub metadata: HashMap<String, HashMap<String, Value>>,
    pub functions: HashMap<String, StructCustomFunction>,
    pub state: StorageState,
    pub snapshot: Option<Box<Snapshot<MemoryStorage>>>,
}
// TODO: Default implementation on MemoryStorage
impl Default for MemoryStorage {
    fn default() -> MemoryStorage {
        Self {
            id_counter: 0,
            items: HashMap::new(),
            metadata: HashMap::new(),
            functions: HashMap::new(),
            state: StorageState::Idle,
            snapshot: None,
        }
    }
}

impl MemoryStorage {
    fn set_snapshot(&mut self) {
        let snapshot = Snapshot::new(self.clone());
        self.snapshot = Some(Box::new(snapshot));
    }
}

#[async_trait(?Send)]
impl CustomFunction for MemoryStorage {
    async fn fetch_function(&self, func_name: &str) -> Result<Option<&StructCustomFunction>> {
        Ok(self.functions.get(&func_name.to_uppercase()))
    }
    async fn fetch_all_functions(&self) -> Result<Vec<&StructCustomFunction>> {
        Ok(self.functions.values().collect())
    }
}

#[async_trait(?Send)]
impl CustomFunctionMut for MemoryStorage {
    async fn insert_function(&mut self, func: StructCustomFunction) -> Result<()> {
        self.functions.insert(func.func_name.to_uppercase(), func);
        Ok(())
    }

    async fn delete_function(&mut self, func_name: &str) -> Result<()> {
        self.functions.remove(&func_name.to_uppercase());
        Ok(())
    }
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

#[async_trait(?Send)]
impl StoreMut for MemoryStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let created = HashMap::from([(
            "CREATED".to_owned(),
            Value::Timestamp(Utc::now().naive_utc()),
        )]);
        let meta = HashMap::from([(schema.table_name.clone(), created)]);
        self.metadata.extend(meta);

        let table_name = schema.table_name.clone();
        let item = Item {
            schema: schema.clone(),
            rows: BTreeMap::new(),
        };
        self.items.insert(table_name, item);

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.items.remove(table_name);
        self.metadata.remove(table_name);

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            for row in rows {
                self.id_counter += 1;

                item.rows.insert(Key::I64(self.id_counter), row);
            }
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            for (key, row) in rows {
                item.rows.insert(key, row);
            }
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            for key in keys {
                item.rows.remove(&key);
            }
        }

        Ok(())
    }
}
