use {
    gluesql_core::{
        ast::DataType,
        data::Key,
        error::{AlterTableError, Error},
    },
    rusqlite::types::Type,
    thiserror::Error as ThisError,
};

#[derive(Debug, ThisError)]
pub enum SqliteStorageError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error(transparent)]
    Glue(#[from] Error),

    #[error(transparent)]
    RonEncode(#[from] ron::Error),

    #[error(transparent)]
    RonDecode(#[from] ron::error::SpannedError),

    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),

    #[error("reserved SQLite storage table name: {0}")]
    ReservedTableName(String),

    #[error("SQLite storage table not found: {0}")]
    TableNotFound(String),

    #[error("missing logical type metadata for {table}.{column}")]
    MissingLogicalType { table: String, column: String },

    #[error("unknown stored logical type: {0}")]
    InvalidLogicalType(String),

    #[error("SQLite schema is not a CREATE TABLE definition")]
    InvalidTableDefinition,

    #[error("CREATE TABLE translation unexpectedly returned another statement")]
    UnreachableCreateTableTranslation,

    #[error("SQLite metadata table has an unexpected definition: {0}")]
    ConflictMetadataSchema(String),

    #[error("unexpected SQLite {actual:?} value for {expected}")]
    InvalidStorageType { expected: DataType, actual: Type },

    #[error("invalid SQLite boolean: {0}")]
    InvalidBoolean(i64),

    #[error("SQLite integer {value} is outside {data_type}")]
    IntegerOutOfRange { data_type: DataType, value: i64 },

    #[error("SQLite REAL cannot preserve NaN")]
    NanNotSupported,

    #[error("RON value has type {actual:?}, expected {expected}")]
    InvalidRonType {
        expected: DataType,
        actual: Option<DataType>,
    },

    #[error("row has {actual} values, expected {expected}")]
    ConflictRowLength { expected: usize, actual: usize },

    #[error("row key {actual:?} does not match the supplied key {expected:?}")]
    ConflictPrimaryKey { expected: Key, actual: Key },

    #[error("rowid requires an I64 key, received {0:?}")]
    InvalidRowidKey(Key),

    #[error("all SQLite rowid aliases are shadowed in {0}")]
    RowidUnavailable(String),

    #[error("invalid scan projection for {0}")]
    InvalidProjection(String),

    #[error("native query must be read-only")]
    NativeStatementNotReadOnly,

    #[error("native query returned {actual} columns, expected {expected}")]
    NativeColumnCountMismatch { expected: usize, actual: usize },

    #[error("nested SQLite transactions are not supported")]
    NestedTransaction,

    #[error("SQLite transaction not found")]
    TransactionNotFound,

    #[error("SQLite storage does not support schema indexes")]
    SchemaIndexesNotSupported,

    #[error("SQLite storage does not support engine selection: {0}")]
    EngineNotSupported(String),

    #[error("cannot change a table between schemaful and schemaless: {0}")]
    SchemaKindChangeNotSupported(String),

    #[error("SQLite table has no columns: {0}")]
    EmptySchema(String),

    #[error("SQLite declared type {actual} does not match {expected}")]
    ConflictColumnType { expected: DataType, actual: String },

    #[error("invalid physical schema for schemaless table: {0}")]
    ConflictSchemalessSchema(String),

    #[error("foreign key violation in {table} at rowid {rowid}, referencing {parent}")]
    ConflictForeignKey {
        table: String,
        rowid: i64,
        parent: String,
    },

    #[error("operation failed ({operation}); foreign key deferral restore failed ({restore})")]
    ForeignKeyDeferralRestore {
        operation: Box<SqliteStorageError>,
        restore: rusqlite::Error,
    },

    #[error("operation failed ({operation}); savepoint cleanup failed ({rollback})")]
    SavepointRollback {
        operation: Box<SqliteStorageError>,
        rollback: rusqlite::Error,
    },

    #[error("automatic commit failed ({commit}); rollback failed ({rollback})")]
    AutoCommitRollback {
        commit: Box<rusqlite::Error>,
        rollback: rusqlite::Error,
    },
}

impl From<AlterTableError> for SqliteStorageError {
    fn from(error: AlterTableError) -> Self {
        Self::Glue(error.into())
    }
}

impl From<SqliteStorageError> for Error {
    fn from(error: SqliteStorageError) -> Self {
        match error {
            SqliteStorageError::Glue(error) => error,
            error => Self::StorageMsg(error.to_string()),
        }
    }
}
