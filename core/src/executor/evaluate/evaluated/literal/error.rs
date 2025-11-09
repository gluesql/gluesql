use {crate::ast::DataType, serde::Serialize, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum LiteralError {
    #[error("literal {literal} is incompatible with data type {data_type:?}")]
    IncompatibleLiteralForDataType {
        data_type: DataType,
        literal: String,
    },
}
