use {
    crate::migration::MigrationError,
    gluesql_core::{
        error::Error,
        store::{AlterTableError, IndexError},
    },
    thiserror::Error as ThisError,
};

#[derive(ThisError, Debug)]
pub enum StorageError {
    #[error("nested transaction is not supported")]
    NestedTransactionNotSupported,

    #[error("transaction not found")]
    TransactionNotFound,

    #[error("cannot create table with reserved name: {0}")]
    ReservedTableName(String),

    #[error("table name cannot contain '/': {0}")]
    TableNameContainsSlash(String),

    #[error("conflict - cannot insert schema: referenced index table does not exist: {0}")]
    ConflictMissingIndexTableOnSchemaInsert(String),

    #[error("conflict - unsupported stored index expression during column rename")]
    ConflictRenamingIndexExpression,

    #[error("conflict - row has no column at position {0}")]
    ConflictDroppingColumnIndex(usize),

    #[error(transparent)]
    AlterTable(#[from] AlterTableError),

    #[error(transparent)]
    Index(#[from] IndexError),

    #[error(transparent)]
    Migration(#[from] MigrationError),

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
    RedbUpgrade(#[from] redb::UpgradeError),

    #[error(transparent)]
    Bincode(#[from] bincode::Error),
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Error {
        match e {
            StorageError::AlterTable(e) => e.into(),
            StorageError::Index(e) => e.into(),
            e => Error::StorageMsg(e.to_string()),
        }
    }
}

impl From<redb::TransactionError> for StorageError {
    fn from(e: redb::TransactionError) -> StorageError {
        StorageError::RedbTransaction(Box::new(e))
    }
}
