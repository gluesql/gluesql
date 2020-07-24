use std::fmt::Debug;
use std::marker::Sized;
use thiserror::Error;

use super::data::{Row, Schema};
use super::result::{MutResult, Result};

#[derive(Error, Debug, PartialEq)]
pub enum StoreError {
    #[error("Schema not found")]
    SchemaNotFound,
}

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

pub trait Store<T: Debug> {
    fn get_schema(&self, table_name: &str) -> Result<Schema>;

    fn get_data(&self, table_name: &str) -> Result<RowIter<T>>;
}

pub trait MutStore<T: Debug>
where
    Self: Sized,
{
    fn gen_id(self, table_name: &str) -> MutResult<Self, T>;

    fn set_schema(self, schema: &Schema) -> MutResult<Self, ()>;

    fn del_schema(self, table_name: &str) -> MutResult<Self, ()>;

    fn set_data(self, key: &T, row: Row) -> MutResult<Self, Row>;

    fn del_data(self, key: &T) -> MutResult<Self, ()>;
}
