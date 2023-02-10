mod alter_table;
mod data_row;
mod index;
mod transaction;

pub trait GStore: Store + Index {}
impl<S: Store + Index> GStore for S {}

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DictionaryView {
    GlueTables,
    GlueTableColumns,
    GlueIndexes,
    GlueObjects,
}

#[async_trait(?Send)]
pub trait Metadata {
    async fn scan_meta(&self, view_name: &DictionaryView) -> Result<RowIter> {
        unimplemented!("unimplemented scan_meta");
    }

    async fn append_meta(&mut self, view_name: &DictionaryView, rows: Vec<DataRow>) -> Result<()> {
        unimplemented!("unimplemented append_meta");
    }

    async fn insert_meta(
        &mut self,
        view_name: &DictionaryView,
        rows: Vec<(Key, DataRow)>,
    ) -> Result<()> {
        unimplemented!("unimplemented insert_meta");
    }

    async fn delete_meta(&mut self, view_name: &DictionaryView, keys: Vec<Key>) -> Result<()> {
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
