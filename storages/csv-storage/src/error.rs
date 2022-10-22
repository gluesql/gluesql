use thiserror::Error as ThisError;

#[derive(ThisError, Debug, PartialEq, Eq)]
pub enum StorageError {
    #[error("cannot import file as table: {0}")]
    InvalidFileImport(String),

    #[error("failed to process csv record: {0}")]
    FailedToProcessCsv(String),

    #[error("given schema doesn't fit for csv table: {0}, {1}")]
    SchemaMismatch(String, String),
}

impl StorageError {
    pub fn from_csv_error(e: csv::Error) -> Self {
        Self::FailedToProcessCsv(e.to_string())
    }
}
