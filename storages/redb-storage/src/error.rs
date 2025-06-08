use {gluesql_core::error::Error, thiserror::Error as ThisError};

#[derive(ThisError, Debug)]
pub enum StorageError {
    #[error("nested transaction is not supported")]
    NestedTransactionNotSupported,
    #[error("transaction not found")]
    TransactionNotFound,

    #[error(transparent)]
    Glue(#[from] Error),

    #[error(transparent)]
    RedbDatabase(#[from] redb::DatabaseError),
    #[error(transparent)]
    RedbStorage(#[from] redb::StorageError),
    #[error(transparent)]
    RedbTable(#[from] redb::TableError),
    #[error(transparent)]
    RedbTransaction(Box<redb::TransactionError>),
    #[error(transparent)]
    RedbCommit(#[from] redb::CommitError),

    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    #[error("cannot create table with reserved name: {0}")]
    ReservedTableName(String),
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Error {
        Error::StorageMsg(e.to_string())
    }
}

impl From<redb::TransactionError> for StorageError {
    fn from(e: redb::TransactionError) -> StorageError {
        StorageError::RedbTransaction(Box::new(e))
    }
}

