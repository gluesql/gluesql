#![deny(clippy::str_to_string)]

use gluesql_core::ast::ColumnDef;

mod alter_table;
mod index;
mod metadata;
mod transaction;
mod undo;

use {
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

type Metadata = HashMap<String, Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageState {
    Idle,
    Transaction { autocommit: bool },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Log {
    InsertSchema(String),
    RenameSchema(String, String),
    DeleteSchema(String, Item, Metadata),
    RenameColumn(String, String, String),
    AddColumn(String, String),
    DropColumn(String, ColumnDef, usize, HashMap<Key, Value>),
    InsertData(String, Vec<Key>),
    UpdateData(String, Vec<(Key, DataRow)>),
    DeleteData(String, Vec<(Key, DataRow)>),
    AppendData(String, Vec<Key>),
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
    pub metadata: HashMap<String, Metadata>,
    pub functions: HashMap<String, StructCustomFunction>,
    pub state: StorageState,
    pub log_buffer: Vec<Log>,
}

impl Default for MemoryStorage {
    fn default() -> MemoryStorage {
        Self {
            id_counter: 0,
            items: HashMap::new(),
            metadata: HashMap::new(),
            functions: HashMap::new(),
            state: StorageState::Idle,
            log_buffer: Vec::new(),
        }
    }
}

impl MemoryStorage {
    pub fn push_log(&mut self, stmt: Log) {
        self.log_buffer.push(stmt);
    }

    pub fn pop_log(&mut self) -> Option<Log> {
        self.log_buffer.pop()
    }

    pub fn clear_buffer(&mut self) {
        self.log_buffer.clear();
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
        if self.functions.get(&func_name.to_uppercase()).is_some() {
            self.functions.remove(&func_name.to_uppercase());
        }
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
        self.push_log(Log::InsertSchema(table_name.clone()));
        self.items.insert(table_name, item);

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        if let (Some(item), Some(metadata)) =
            (self.items.get(table_name), self.metadata.get(table_name))
        {
            self.push_log(Log::DeleteSchema(
                table_name.to_owned(),
                item.to_owned(),
                metadata.to_owned(),
            ));

            self.items.remove(table_name);
            self.metadata.remove(table_name);
        }
        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            let mut key_vec: Vec<Key> = Vec::new();
            for row in rows {
                self.id_counter += 1;
                let id = self.id_counter;

                item.rows.insert(Key::I64(id), row);
                key_vec.push(Key::I64(id));
            }
            self.push_log(Log::AppendData(table_name.to_owned(), key_vec));
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            let mut keys: Vec<Key> = Vec::new();
            let mut history: Vec<(Key, DataRow)> = Vec::new();

            for (key, row) in rows {
                if let Some(old_row) = item.rows.insert(key.clone(), row) {
                    history.push((key, old_row));
                } else {
                    keys.push(key.clone());
                }
            }
            println!("keys : {:?}", keys);
            println!("history: {:?}", history);
            match keys.is_empty() {
                true => self.push_log(Log::UpdateData(table_name.to_owned(), history)),
                false => self.push_log(Log::InsertData(table_name.to_owned(), keys)),
            }
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            let mut data_rows: Vec<(Key, DataRow)> = Vec::new();
            for key in keys {
                if let Some(row) = item.rows.get(&key) {
                    data_rows.push((key.clone(), row.clone()));
                }
                item.rows.remove(&key);
            }
            self.push_log(Log::DeleteData(table_name.to_owned(), data_rows));
        }

        Ok(())
    }
}
