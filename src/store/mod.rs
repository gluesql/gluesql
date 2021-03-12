#[cfg(feature = "alter-table")]
mod alter_table;
#[cfg(feature = "alter-table")]
pub use alter_table::*;
#[cfg(not(feature = "alter-table"))]
pub trait AlterTable {}

use async_trait::async_trait;
use std::fmt::Debug;
use std::marker::Sized;

use crate::data::{Row, Schema};
use crate::result::{MutResult, Result};

#[cfg(feature = "auto-increment")]
use crate::data::Value;

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

/// By implementing `Store` trait, you can run `SELECT` queries.
#[async_trait(?Send)]
pub trait Store<T: Debug> {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<T>>;

    #[cfg(feature = "auto-increment")]
    async fn get_generator(&self, table_name: &str, column_name: &str) -> Result<Value>;
}

/// `StoreMut` takes role of mutation, related to `INSERT`, `CREATE`, `DELETE`, `DROP` and
/// `UPDATE`.
#[async_trait(?Send)]
pub trait StoreMut<T: Debug>
where
    Self: Sized,
{
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()>;

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()>;

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()>;

    async fn update_data(self, rows: Vec<(T, Row)>) -> MutResult<Self, ()>;

    async fn delete_data(self, keys: Vec<T>) -> MutResult<Self, ()>;

    #[cfg(feature = "auto-increment")]
    async fn set_generator(
        self,
        table_name: &str,
        column_name: &str,
        value: Value,
    ) -> MutResult<Self, ()>;
}
