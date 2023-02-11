use {gluesql_core::result::Error, thiserror::Error as ThisError};

#[derive(ThisError, Debug, PartialEq, Eq)]
pub enum StorageError {
    #[error("failed to create table file")]
    FailedToCreateTableFile,

    #[error("cannot import file as table: {0}")]
    InvalidFileImport(String),

    #[error("failed to append data: {0}")]
    FailedToAppendData(String),

    #[error("failed to process csv record: {0}")]
    FailedToProcessCsv(String),

    #[error("given schema doesn't fit for csv table: {0}, {1}")]
    SchemaMismatch(String, String),

    #[error("given row does not fit for column definition")]
    ColumnDefMismatch,

    #[error("cannot read schema file: {0}")]
    InvalidSchemaFile(String),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("schema-less operation not supported yet")]
    SchemaLessNotSupported,
}

impl StorageError {
    pub fn from_csv_error(e: csv::Error) -> Self {
        Self::FailedToProcessCsv(e.to_string())
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Self::Storage(Box::new(e))
    }
}
