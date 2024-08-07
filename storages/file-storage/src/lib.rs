#![deny(clippy::str_to_string)]

use {
    async_trait::async_trait,
    futures::stream::iter,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata,
            RowIter, Store, StoreMut, Transaction,
        },
    },
    serde::{Deserialize, Serialize},
    std::collections::{BTreeMap, HashMap},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub schema: Schema,
    pub rows: BTreeMap<Key, DataRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileStorage {
    pub id_counter: i64,
    pub items: HashMap<String, Item>,
}

impl FileStorage {
    pub fn scan_data(&self, table_name: &str) -> Vec<(Key, DataRow)> {
        match self.items.get(table_name) {
            Some(item) => item.rows.clone().into_iter().collect(),
            None => vec![],
        }
    }
}

#[async_trait(?Send)]
impl Store for FileStorage {
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
            .and_then(|item| item.rows.get(key).cloned());

        Ok(row)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let rows = FileStorage::scan_data(self, table_name).into_iter().map(Ok);

        Ok(Box::pin(iter(rows)))
    }
}

#[async_trait(?Send)]
impl StoreMut for FileStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
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

impl AlterTable for FileStorage {}
impl Index for FileStorage {}
impl IndexMut for FileStorage {}
impl Transaction for FileStorage {}
impl Metadata for FileStorage {}
impl CustomFunction for FileStorage {}
impl CustomFunctionMut for FileStorage {}
