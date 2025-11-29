use {crate::ast::DataType, serde::Serialize, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum LiteralError {
    #[error("failed to parse number {literal:?} to {data_type}")]
    NumberParseFailed {
        literal: String,
        data_type: DataType,
    },

    #[error("failed to cast number {literal:?} to {data_type}")]
    NumberCastFailed {
        literal: String,
        data_type: DataType,
    },

    #[error("failed to parse text {literal:?} to {data_type}")]
    TextParseFailed {
        literal: String,
        data_type: DataType,
    },

    #[error("failed to cast text {literal:?} to {data_type}")]
    TextCastFailed {
        literal: String,
        data_type: DataType,
    },
}
