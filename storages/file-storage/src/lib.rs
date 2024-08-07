#![deny(clippy::str_to_string)]

mod store;
mod store_mut;

use {
    gluesql_core::{
        data::{Key, Schema},
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata,
            Transaction,
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

impl AlterTable for FileStorage {}
impl Index for FileStorage {}
impl IndexMut for FileStorage {}
impl Transaction for FileStorage {}
impl Metadata for FileStorage {}
impl CustomFunction for FileStorage {}
impl CustomFunctionMut for FileStorage {}
