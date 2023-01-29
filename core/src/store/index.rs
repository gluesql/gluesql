#![cfg(feature = "index")]

use {
    super::RowIter,
    crate::{
        ast::{IndexOperator, OrderByExpr},
        data::Value,
        result::Result,
    },
    async_trait::async_trait,
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum IndexError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("index name already exists: {0}")]
    IndexNameAlreadyExists(String),

    #[error("index name does not exist: {0}")]
    IndexNameDoesNotExist(String),

    #[error("conflict - table not found: {0}")]
    ConflictTableNotFound(String),

    #[error("conflict - update failed - index value")]
    ConflictOnEmptyIndexValueUpdate,

    #[error("conflict - delete failed - index value")]
    ConflictOnEmptyIndexValueDelete,

    #[error("conflict - scan failed - index value")]
    ConflictOnEmptyIndexValueScan,

    #[error("conflict - index sync - delete index data")]
    ConflictOnIndexDataDeleteSync,
}

#[async_trait]
pub trait Index {
    async fn scan_indexed_data(
        &self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter>;
}

#[async_trait]
pub trait IndexMut {
    async fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> Result<()>;

    async fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()>;
}
