use {gluesql_core::error::Error, thiserror::Error as ThisError};

#[derive(Debug, ThisError)]
pub(super) enum StorageError {
    #[error("nested transaction is not supported")]
    NestedTransactionNotSupported,

    #[error("transaction not found")]
    TransactionNotFound,

    #[error("table name cannot contain '/': {0}")]
    TableNameContainsSlash(String),

    #[error("failed to create SlateDB runtime: {0}")]
    Runtime(#[source] std::io::Error),

    #[error(transparent)]
    SlateDb(#[from] slatedb::Error),

    #[error("SlateDB commit failed: {0}")]
    Commit(#[source] slatedb::Error),

    #[error("SlateDB commit durability could not be confirmed: {0}")]
    Durability(#[source] slatedb::Error),

    #[error(transparent)]
    Bincode(#[from] bincode::Error),
}

impl From<StorageError> for Error {
    fn from(error: StorageError) -> Self {
        Self::StorageMsg(error.to_string())
    }
}
