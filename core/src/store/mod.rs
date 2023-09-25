mod alter_table;
mod data_row;
mod function;
mod index;
mod metadata;
mod transaction;

pub use {
    crate::{
        data::{Key, Schema},
        result::Result,
    },
    alter_table::{AlterTable, AlterTableError},
    async_trait::async_trait,
    data_row::DataRow,
    function::{CustomFunction, CustomFunctionMut},
    index::{Index, IndexError, IndexMut},
    metadata::{MetaIter, Metadata},
    transaction::Transaction,
};

/// Type enabling streaming of table data.
pub type RowIter = Box<dyn Iterator<Item = Result<(Key, DataRow)>>>;

/// Enables SELECT queries for you database.
#[async_trait(?Send)]
pub trait Store {
    /// Fetch the [`Schema`] of a given table.
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    /// Fetch [`Schema`]s of every talbe in your database.
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    /// Retrive [`DataRow`] from a specific table.
    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>>;

    /// Retrie a [`RowIter`] over the rows of a specific table.
    async fn scan_data(&self, table_name: &str) -> Result<RowIter>;
}

/// Enables INSERT, CREATE TABLE, DELETE, UPDATE, and DROP TABLES operations for you database.
#[async_trait(?Send)]
pub trait StoreMut {
    /// Creates a table according to the given [Schema]
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()>;

    /// Deletes table of the given name.
    async fn delete_schema(&mut self, table_name: &str) -> Result<()>;

    /// Insert new data to exisiting table without primary key index.
    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()>;

    /// Insert new data to exisitng table having primary key index.
    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()>;

    /// Delete data of exisitng table corresponding to given primary key index.
    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()>;
}

/// Composite trait representing a databse backend that supports all information retrieval operations.
pub trait GStore: Store + Index + Metadata + CustomFunction {}
impl<S: Store + Index + Metadata + CustomFunction> GStore for S {}

/// Composite trait representing a databse backend that supports database modification operations.
pub trait GStoreMut:
    StoreMut + IndexMut + AlterTable + Transaction + CustomFunction + CustomFunctionMut
{
}
impl<S: StoreMut + IndexMut + AlterTable + Transaction + CustomFunction + CustomFunctionMut>
    GStoreMut for S
{
}
