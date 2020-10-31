#[cfg(feature = "alter-table")]
mod alter_table;
#[cfg(feature = "alter-table")]
pub use alter_table::*;
#[cfg(not(feature = "alter-table"))]
pub trait AlterTable {}

use std::fmt::Debug;
use std::marker::Sized;

use crate::data::{Row, Schema};
use crate::result::{MutResult, Result};

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

/// By implementing `Store` trait, you can run `SELECT` queries.
pub trait Store<T: Debug> {
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    fn scan_data(&self, table_name: &str) -> Result<RowIter<T>>;
}

/// `StoreMut` takes role of mutation, related to `INSERT`, `CREATE`, `DELETE`, `DROP` and
/// `UPDATE`.
pub trait StoreMut<T: Debug>
where
    Self: Sized,
{
    fn generate_id(self, table_name: &str) -> MutResult<Self, T>;

    fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()>;

    fn delete_schema(self, table_name: &str) -> MutResult<Self, ()>;

    fn insert_data(self, key: &T, row: Row) -> MutResult<Self, ()>;

    fn delete_data(self, key: &T) -> MutResult<Self, ()>;
}
