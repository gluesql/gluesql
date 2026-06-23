#![deny(clippy::str_to_string)]

mod alter_table;
mod index;
mod metadata;
mod transaction;

use {
    gluesql_core::{
        chrono::Utc,
        data::{CustomFunction as StructCustomFunction, Key, Schema, Value},
        error::Result,
        store::{CustomFunction, CustomFunctionMut, Planner, RowIter, Store, StoreMut},
    },
    serde::{Deserialize, Serialize},
    std::collections::{BTreeMap, HashMap},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub schema: Schema,
    pub rows: BTreeMap<Key, Vec<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryStorage {
    pub id_counter: i64,
    pub items: HashMap<String, Item>,
    pub metadata: HashMap<String, BTreeMap<String, Value>>,
    pub functions: HashMap<String, StructCustomFunction>,
}

impl MemoryStorage {
    pub fn scan_data(&self, table_name: &str) -> Vec<(Key, Vec<Value>)> {
        match self.items.get(table_name) {
            Some(item) => item.rows.clone().into_iter().collect(),
            None => vec![],
        }
    }
}

impl CustomFunction for MemoryStorage {
    fn fetch_function<'a>(&'a self, func_name: &str) -> Result<Option<&'a StructCustomFunction>> {
        Ok(self.functions.get(&func_name.to_uppercase()))
    }
    fn fetch_all_functions(&self) -> Result<Vec<&StructCustomFunction>> {
        Ok(self.functions.values().collect())
    }
}

impl CustomFunctionMut for MemoryStorage {
    fn insert_function(&mut self, func: StructCustomFunction) -> Result<()> {
        self.functions.insert(func.func_name.to_uppercase(), func);
        Ok(())
    }

    fn delete_function(&mut self, func_name: &str) -> Result<()> {
        self.functions.remove(&func_name.to_uppercase());
        Ok(())
    }
}

impl Store for MemoryStorage {
    fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut schemas = self
            .items
            .values()
            .map(|item| item.schema.clone())
            .collect::<Vec<_>>();
        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.items
            .get(table_name)
            .map(|item| Ok(item.schema.clone()))
            .transpose()
    }

    fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        let row = self
            .items
            .get(table_name)
            .and_then(|item| item.rows.get(key).cloned());

        Ok(row)
    }

    fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let rows = MemoryStorage::scan_data(self, table_name)
            .into_iter()
            .map(Ok);

        Ok(Box::new(rows))
    }
}

impl StoreMut for MemoryStorage {
    fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let created = BTreeMap::from([(
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

    fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.items.remove(table_name);
        self.metadata.remove(table_name);

        Ok(())
    }

    fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            for row in rows {
                self.id_counter += 1;

                item.rows.insert(Key::I64(self.id_counter), row);
            }
        }

        Ok(())
    }

    fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            for (key, row) in rows {
                item.rows.insert(key, row);
            }
        }

        Ok(())
    }

    fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            for key in keys {
                item.rows.remove(&key);
            }
        }

        Ok(())
    }
}

impl Planner for MemoryStorage {}
