use nom_sql::CreateTableStatement;
use std::fmt::Debug;
use thiserror::Error;

use crate::data::{Row, Schema};
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum StoreError {
    #[error("Schema not found")]
    SchemaNotFound,
}

pub type RowIter<T> = Box<dyn Iterator<Item = Result<(T, Row)>>>;

pub trait Store<T: Debug> {
    fn gen_id(&self, table_name: &str) -> Result<T>;

    fn set_schema(&self, schema: &Schema) -> Result<()>;

    fn get_schema(&self, table_name: &str) -> Result<CreateTableStatement>;
    fn get_schema2(&self, table_name: &str) -> Result<Schema>;

    fn set_data(&self, key: &T, row: Row) -> Result<Row>;

    fn get_data(&self, table_name: &str) -> Result<RowIter<T>>;

    fn del_data(&self, key: &T) -> Result<()>;
}
