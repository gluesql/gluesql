use {gluesql_core::error::Error, thiserror::Error as ThisError};

#[derive(ThisError, Debug)]
pub enum StorageError {
    #[error("nested transaction is not supported")]
    NestedTransactionNotSupported,
    #[error("transaction not found")]
    TransactionNotFound,
    #[error("cannot create table with reserved name: {0}")]
    ReservedTableName(String),
    #[error(
        "[RedbStorage] migration required (found v{found}, expected v{expected}); migrate redb-storage data to the latest format before opening"
    )]
    MigrationRequired { found: u32, expected: u32 },
    #[error("[RedbStorage] unsupported format version v{0}")]
    UnsupportedFormatVersion(u32),
    #[error("[RedbStorage] unsupported newer format version v{0}")]
    UnsupportedNewerFormatVersion(u32),
    #[error("[RedbStorage] invalid storage format metadata: missing format version key")]
    MissingFormatVersionMetadata,
    #[error("[RedbStorage] failed to parse v1 row payload in table '{0}'")]
    InvalidV1RowPayload(String),

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
