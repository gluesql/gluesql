use std::collections::HashMap;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::{
    ast::ColumnDef,
    prelude::{DataType, Value},
};

mod alter_table;
mod data_row;
mod index;
mod transaction;

pub trait GStore: Store + Index + Metadata {}
impl<S: Store + Index + Metadata> GStore for S {}

pub trait GStoreMut: StoreMut + IndexMut + AlterTable + Transaction {}
impl<S: StoreMut + IndexMut + AlterTable + Transaction> GStoreMut for S {}

pub use {
    alter_table::{AlterTable, AlterTableError},
    data_row::DataRow,
    index::{Index, IndexError, IndexMut},
    transaction::Transaction,
};

use {
    crate::{
        data::{Key, Schema},
        result::Result,
    },
    async_trait::async_trait,
    serde::{Deserialize, Serialize},
    strum_macros::Display,
};

pub type RowIter = Box<dyn Iterator<Item = Result<(Key, DataRow)>>>;

/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
pub trait Store {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter>;
}

// #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
// pub struct Meta {
//     pub name: MetaName,
//     pub row: MetaRow,
// }

// impl MetaRow {
//     pub fn to_values(self) -> DataRow {
//         match self {
//             MetaRow::GlueObjects(glue_object) => glue_object.to_values(),
//             MetaRow::GlueTables(glue_tables) => todo!(),
//             MetaRow::GlueDatabases(glue_databases) => todo!(),
//         }
//     }
// }

#[async_trait(?Send)]
pub trait Metadata {
    async fn scan_meta(&self, meta_name: &str) -> Value {
        unimplemented!("unimplemented scan_meta");
    }

    async fn scan_all_metas(&self) -> HashMap<String, Value> {
        unimplemented!("unimplemented scan_meta");
    }

    async fn append_meta(&mut self, meta: HashMap<String, Value>) -> Result<()> {
        unimplemented!("unimplemented append_meta");
    }

    // async fn insert_meta(&mut self, meta: HashMap<String, Value>) -> Result<()> {
    //     unimplemented!("unimplemented insert_meta");
    // }

    async fn delete_meta(&mut self, meta_name: &str) -> Result<()> {
        unimplemented!("unimplemented delete_meta");
    }
}

/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
#[async_trait(?Send)]
pub trait StoreMut {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()>;

    async fn delete_schema(&mut self, table_name: &str) -> Result<()>;

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()>;

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()>;

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()>;
}
