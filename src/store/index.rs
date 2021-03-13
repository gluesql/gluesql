use crate::Condition;
use crate::MutResult;
use async_trait::async_trait;
use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

use crate::data::Row;
use crate::Result;
use crate::RowIter;
use sled::IVec;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum IndexError {
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("The index {0} already exists")]
    IndexAlreadyExists(String),
    #[error("The index {0} does not exists")]
    IndexNotFound(String),
    #[error("Duplicates are not allowed on an UNIQUE index, some were found in {0}.")]
    UniqueIndexContainsDuplicates(String),
    #[error("The row {0} was not found")]
    RowNotFound(String),
    #[error("Unimplemented: {0}")]
    Unimplemented(String),
}

#[async_trait(?Send)]
pub trait Index
where
    Self: Sized,
{
    /// Creates one or more index(es)
    /// Please not that if unique is set to true and there is a duplicate row,
    /// you have to return [crate::store::index::IndexError::UniqueIndexContainsDuplicates].
    async fn create(
        self,
        table_name: &str,
        row_names: Vec<&str>,
        unique: bool,
    ) -> MutResult<Self, ()>;

    /// Drops one or more already created index(es)
    async fn drop(self, table_name: &str, row_names: Vec<&str>) -> MutResult<Self, ()>;

    async fn get_by_key<T: Debug>(&self, table_name: &str, key: IVec) -> Result<Row>;

    async fn get_indexed_keys<T: Debug>(
        &self,
        condition: Condition,
        table_name: &str,
    ) -> Result<Vec<IVec>>;
}
