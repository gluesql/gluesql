use nom_sql::CreateTableStatement;
use std::fmt::Debug;
use thiserror::Error;

use crate::data::Row;
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum StoreError {
    #[error("Schema not found")]
    SchemaNotFound,
}

pub trait Store<T: std::fmt::Debug> {
    fn gen_id(&self, table_name: &str) -> Result<T>;

    fn set_schema(&self, statement: &CreateTableStatement) -> Result<()>;

    fn get_schema(&self, table_name: &str) -> Result<CreateTableStatement>;

    fn set_data(&self, key: &T, row: Row) -> Result<Row>;

    fn get_data(&self, table_name: &str) -> Result<Box<dyn Iterator<Item = Result<(T, Row)>>>>;

    fn del_data(&self, key: &T) -> Result<()>;
}
