#[cfg(feature = "alter-table")]
mod alter_table;
#[cfg(feature = "alter-table")]
pub use alter_table::*;
#[cfg(not(feature = "alter-table"))]
pub trait AlterTable {}

#[cfg(feature = "index")]
mod index;
#[cfg(feature = "index")]
pub use index::IndexError;

use {
    crate::{
        data::{Row, Schema},
        result::{MutResult, Result},
    },
    async_trait::async_trait,
    std::fmt::Debug,
};

#[cfg(feature = "index")]
use crate::{
    ast::{Expr, IndexOperator},
    data::Value,
};

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

/// By implementing `Store` trait, you can run `SELECT` queries.
#[async_trait(?Send)]
pub trait Store<T: Debug> {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<T>>;

    #[cfg(feature = "index")]
    async fn scan_indexed_data(
        &self,
        table_name: &str,
        index_name: &str,
        op: &IndexOperator,
        value: Value,
    ) -> Result<RowIter<T>>;
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

    async fn update_data(self, table_name: &str, rows: Vec<(T, Row)>) -> MutResult<Self, ()>;

    async fn delete_data(self, table_name: &str, keys: Vec<T>) -> MutResult<Self, ()>;

    #[cfg(feature = "index")]
    async fn create_index(
        self,
        table_name: &str,
        index_name: &str,
        column: &Expr,
    ) -> MutResult<Self, ()>;

    #[cfg(feature = "index")]
    async fn drop_index(self, table_name: &str, index_name: &str) -> MutResult<Self, ()>;
}
