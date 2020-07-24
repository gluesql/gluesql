use std::fmt::Debug;
use std::marker::Sized;
use thiserror::Error;

use super::data::{Row, Schema};
use super::result::{GlueResult, Result};

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
    fn gen_id(self, table_name: &str) -> GlueResult<Self, T>;

    fn set_schema(self, schema: &Schema) -> GlueResult<Self, ()>;

    fn del_schema(self, table_name: &str) -> GlueResult<Self, ()>;

    fn set_data(self, key: &T, row: Row) -> GlueResult<Self, Row>;

    fn del_data(self, key: &T) -> GlueResult<Self, ()>;
}
