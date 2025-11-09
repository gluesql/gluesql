use {crate::ast::DataType, serde::Serialize, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum LiteralError {
    #[error("literal {literal} is incompatible with data type {data_type:?}")]
    IncompatibleLiteralForDataType {
        data_type: DataType,
        literal: String,
    },

    #[error("failed to parse number")]
    FailedToParseNumber,

    #[error("unreachable failure on parsing number")]
    UnreachableNumberParsing,

    #[error("failed to parse decimal: {0}")]
    FailedToParseDecimal(String),

    #[error("failed to parse hex string: {0}")]
    FailedToParseHexString(String),

    #[error("failed to parse inet string: {0}")]
    FailedToParseInetString(String),

    #[error("failed to parse date: {0}")]
    FailedToParseDate(String),

    #[error("failed to parse timestamp: {0}")]
    FailedToParseTimestamp(String),

    #[error("failed to parse time: {0}")]
    FailedToParseTime(String),

    #[error("failed to parse point: {0}")]
    FailedToParsePoint(String),

    #[error("literal cast failed to boolean: {0}")]
    LiteralCastToBooleanFailed(String),

    #[error("literal cast failed from text to integer: {0}")]
    LiteralCastFromTextToIntegerFailed(String),

    #[error("literal cast failed from text to Unsigned Int(8): {0}")]
    LiteralCastFromTextToUnsignedInt8Failed(String),

    #[error("literal cast failed from text to UINT16: {0}")]
    LiteralCastFromTextToUint16Failed(String),

    #[error("literal cast failed from text to UINT32: {0}")]
    LiteralCastFromTextToUint32Failed(String),

    #[error("literal cast failed from text to UINT64: {0}")]
    LiteralCastFromTextToUint64Failed(String),

    #[error("literal cast failed from text to UINT128: {0}")]
    LiteralCastFromTextToUint128Failed(String),

    #[error("literal cast failed from text to float: {0}")]
    LiteralCastFromTextToFloatFailed(String),

    #[error("literal cast failed from text to decimal: {0}")]
    LiteralCastFromTextToDecimalFailed(String),

    #[error("literal cast failed to date: {0}")]
    LiteralCastToDateFailed(String),

    #[error("literal cast failed to time: {0}")]
    LiteralCastToTimeFailed(String),

    #[error("literal cast failed to timestamp: {0}")]
    LiteralCastToTimestampFailed(String),

    #[error("literal cast failed from {1} to {0:?}")]
    LiteralCastToDataTypeFailed(DataType, String),

    #[error("literal cast failed to Int(8): {0}")]
    LiteralCastToInt8Failed(String),

    #[error("literal cast failed to Unsigned Int(8): {0}")]
    LiteralCastToUnsignedInt8Failed(String),

    #[error("literal cast failed to UINT16: {0}")]
    LiteralCastToUint16Failed(String),

    #[error("literal cast failed to UNIT32: {0}")]
    LiteralCastToUint32Failed(String),

    #[error("literal cast failed to UNIT64: {0}")]
    LiteralCastToUint64Failed(String),

    #[error("literal cast failed to UNIT128: {0}")]
    LiteralCastToUint128Failed(String),

    #[error("unreachable literal cast from number to float: {0}")]
    UnreachableLiteralCastFromNumberToFloat(String),

    #[error("unreachable literal cast from number to integer: {0}")]
    UnreachableLiteralCastFromNumberToInteger(String),
}
