use {
    super::RowIter,
    crate::{
        ast::{Expr, IndexOperator},
        data::Value,
        result::{MutResult, Result},
    },
    async_trait::async_trait,
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum IndexError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("index name already exists: {0}")]
    IndexNameAlreadyExists(String),

    #[error("index name does not exist: {0}")]
    IndexNameDoesNotExist(String),

    #[error("conflict - update failed - index value")]
    ConflictOnEmptyIndexValueUpdate,

    #[error("conflict - delete failed - index value")]
    ConflictOnEmptyIndexValueDelete,

    #[error("conflict - scan failed - index value")]
    ConflictOnEmptyIndexValueScan,

    #[error("conflict - index sync - delete index data")]
    ConflictOnIndexDataDeleteSync,
}

#[async_trait(?Send)]
pub trait Index<T: Debug> {
    async fn scan_indexed_data(
        &self,
        table_name: &str,
        index_name: &str,
        op: &IndexOperator,
        value: Value,
    ) -> Result<RowIter<T>>;
}

#[async_trait(?Send)]
pub trait IndexMut<T: Debug>
where
    Self: Sized,
{
    async fn create_index(
        self,
        table_name: &str,
        index_name: &str,
        column: &Expr,
    ) -> MutResult<Self, ()>;

    async fn drop_index(self, table_name: &str, index_name: &str) -> MutResult<Self, ()>;
}
