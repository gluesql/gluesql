use serde::Serialize;
use std::fmt::Debug;
use std::marker::Sized;
use thiserror::Error;

use super::data::{Row, Schema};
use super::result::{MutResult, Result};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum StoreError {
    #[error("Schema not found")]
    SchemaNotFound,
}

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

pub trait Store<T: Debug> {
    fn fetch_schema(&self, table_name: &str) -> Result<Schema>;

    fn scan_data(&self, table_name: &str) -> Result<RowIter<T>>;
}

pub trait MutStore<T: Debug>
where
    Self: Sized,
{
    fn generate_id(self, table_name: &str) -> MutResult<Self, T>;

    fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()>;

    fn delete_schema(self, table_name: &str) -> MutResult<Self, ()>;

    fn insert_data(self, key: &T, row: Row) -> MutResult<Self, Row>;

    fn delete_data(self, key: &T) -> MutResult<Self, ()>;
}
