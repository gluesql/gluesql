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

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

/// By implementing `Store` trait, you can run `SELECT` queries.
#[async_trait]
pub trait Store<T: Debug> {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<T>>;
}

/// `StoreMut` takes role of mutation, related to `INSERT`, `CREATE`, `DELETE`, `DROP` and
/// `UPDATE`.
#[async_trait]
pub trait StoreMut<T: Debug>
where
    Self: Sized,
{
    async fn generate_id(self, table_name: &str) -> MutResult<Self, T>;

    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()>;

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()>;

    async fn insert_data(self, key: &T, row: Row) -> MutResult<Self, ()>;

    async fn delete_data(self, key: &T) -> MutResult<Self, ()>;
}
