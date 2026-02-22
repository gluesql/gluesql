#[derive(thiserror::Error, Debug, uniffi::Error)]
#[uniffi(flat_error)]
pub enum GlueSQLError {
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Execute error: {0}")]
    ExecuteError(String),
    #[error("Translate error: {0}")]
    TranslateError(String),
    #[error("Plan error: {0}")]
    PlanError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Value error: {0}")]
    ValueError(String),
}
