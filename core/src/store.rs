mod alter_table;
mod function;
mod index;
mod metadata;
mod planner;
mod transaction;

pub trait GStore: Store + Index + Metadata + CustomFunction {}
impl<S: Store + Index + Metadata + CustomFunction> GStore for S {}

pub trait GStoreMut:
    StoreMut + IndexMut + AlterTable + Transaction + CustomFunction + CustomFunctionMut
{
}
impl<S: StoreMut + IndexMut + AlterTable + Transaction + CustomFunction + CustomFunctionMut>
    GStoreMut for S
{
}

pub use {
    alter_table::{AlterTable, AlterTableError},
    function::{CustomFunction, CustomFunctionMut},
    index::{Index, IndexError, IndexMut},
    metadata::{MetaIter, Metadata},
    planner::Planner,
    transaction::Transaction,
};

use crate::{
    data::{Key, Schema, Value},
    result::{Error, Result},
};

pub type RowIter<'a> = Box<dyn Iterator<Item = Result<(Key, Vec<Value>)>> + 'a>;

/// Original column name and zero-based position in the full row.
pub type ScanColumn = (String, usize);

/// By implementing `Store` trait, you can run `SELECT` query.
pub trait Store {
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>>;

    /// Scans rows, returning the requested columns in request order.
    ///
    /// `None` returns all columns. `Some(&[])` returns empty values for each row.
    /// Column selection preserves the rows and their keys. Each requested name
    /// and position must identify the same column in the full table schema.
    /// Schemaless tables use `None` and return complete documents.
    ///
    /// The request is not borrowed by the returned iterator. Implementations
    /// retain any positions or reader projection needed during iteration.
    fn scan_data<'a>(
        &'a self,
        table_name: &str,
        columns: Option<&[ScanColumn]>,
    ) -> Result<RowIter<'a>>;
}

/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
pub trait StoreMut {
    fn insert_schema(&mut self, _schema: &Schema) -> Result<()> {
        let msg = "[Storage] StoreMut::insert_schema is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    fn delete_schema(&mut self, _table_name: &str) -> Result<()> {
        let msg = "[Storage] StoreMut::delete_schema is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    fn append_data(&mut self, _table_name: &str, _rows: Vec<Vec<Value>>) -> Result<()> {
        let msg = "[Storage] StoreMut::append_data is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    fn insert_data(&mut self, _table_name: &str, _rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        let msg = "[Storage] StoreMut::insert_data is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    fn delete_data(&mut self, _table_name: &str, _keys: Vec<Key>) -> Result<()> {
        let msg = "[Storage] StoreMut::delete_data is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
}
