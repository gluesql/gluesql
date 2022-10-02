use {
    crate::{ast::DataType, ast::DateTimeField, data::Value},
    serde::Serialize,
    std::fmt::Debug,
    strum_macros::Display,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum ValueError {
    #[error("literal: {literal} is incompatible with data type: {data_type:?}")]
    IncompatibleLiteralForDataType {
        data_type: DataType,
        literal: String,
    },

    #[error("incompatible data type, data type: {data_type:#?}, value: {value:#?}")]
    IncompatibleDataType { data_type: DataType, value: Value },

    #[error("null value on not null field")]
    NullValueOnNotNullField,

    #[error("failed to parse number")]
    FailedToParseNumber,

    #[error("failed to convert Float to Decimal: {0}")]
    FloatToDecimalConversionFailure(f64),

    #[error("failed to parse date: {0}")]
    FailedToParseDate(String),

    #[error("failed to parse timestamp: {0}")]
    FailedToParseTimestamp(String),

    #[error("failed to parse time: {0}")]
    FailedToParseTime(String),

    #[error("failed to UUID: {0}")]
    FailedToParseUUID(String),

    #[error("failed to parse Decimal: {0}")]
    FailedToParseDecimal(String),

    #[error("failed to parse hex string: {0}")]
    FailedToParseHexString(String),

    #[error("non-numeric values {lhs:?} {operator} {rhs:?}")]
    NonNumericMathOperation {
        lhs: Value,
        rhs: Value,
        operator: NumericBinaryOperator,
    },

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("unary plus operation for non numeric value")]
    UnaryPlusOnNonNumeric,

    #[error("unary minus operation for non numeric value")]
    UnaryMinusOnNonNumeric,

    #[error("unary factorial operation for non numeric value")]
    FactorialOnNonNumeric,

    #[error("unary factorial operation for non integer value")]
    FactorialOnNonInteger,

    #[error("unary factorial operation for negative numeric value")]
    FactorialOnNegativeNumeric,

    #[error("unary factorial operation overflow")]
    FactorialOverflow,

    #[error("unreachable failure on parsing number")]
    UnreachableNumberParsing,

    // Cast errors from value to value
    #[error("impossible cast")]
    ImpossibleCast,

    #[error("unimplemented cast")]
    UnimplementedCast,

    // Cast errors from literal to value
    #[error("literal cast failed from text to integer: {0}")]
    LiteralCastFromTextToIntegerFailed(String),

    #[error("literal cast failed from text to Unsigned Int(8): {0}")]
    LiteralCastFromTextToUnsignedInt8Failed(String),

    #[error("literal cast failed from text to float: {0}")]
    LiteralCastFromTextToFloatFailed(String),

    #[error("literal cast failed from text to decimal: {0}")]
    LiteralCastFromTextToDecimalFailed(String),

    #[error("literal cast failed to boolean: {0}")]
    LiteralCastToBooleanFailed(String),

    #[error("literal cast failed to date: {0}")]
    LiteralCastToDateFailed(String),

    #[error("literal cast from {1} to {0} failed")]
    LiteralCastToDataTypeFailed(DataType, String),

    #[error("literal cast failed to Int(8): {0}")]
    LiteralCastToInt8Failed(String),

    #[error("literal cast failed to Unsigned Int(8): {0}")]
    LiteralCastToUnsignedInt8Failed(String),

    #[error("literal cast failed to time: {0}")]
    LiteralCastToTimeFailed(String),

    #[error("literal cast failed to timestamp: {0}")]
    LiteralCastToTimestampFailed(String),

    #[error("unreachable literal cast from number to integer: {0}")]
    UnreachableLiteralCastFromNumberToInteger(String),

    #[error("unreachable literal cast from number to float: {0}")]
    UnreachableLiteralCastFromNumberToFloat(String),

    #[error("unimplemented literal cast: {literal} as {data_type:?}")]
    UnimplementedLiteralCast {
        data_type: DataType,
        literal: String,
    },

    #[error("unreachable integer overflow: {0}")]
    UnreachableIntegerOverflow(String),

    #[error("operator doesn't exist: {0:?} LIKE {1:?}")]
    LikeOnNonString(Value, Value),

    #[error("extract format not matched: {value:?} FROM {field:?})")]
    ExtractFormatNotMatched { value: Value, field: DateTimeField },

    #[error("operator doesn't exist: {0:?} ILIKE {1:?}")]
    ILikeOnNonString(Value, Value),

    #[error("big endian export not supported for {0} type")]
    BigEndianExportNotSupported(String),

    #[error("invalid json string")]
    InvalidJsonString,

    #[error("json object type is required")]
    JsonObjectTypeRequired,

    #[error("json array type is required")]
    JsonArrayTypeRequired,

    #[error("unreachable - failed to parse json number: {0}")]
    UnreachableJsonNumberParseFailure(String),

    #[error("selector requires MAP or LIST types")]
    SelectorRequiresMapOrListTypes,

    #[error("overflow occurred: {lhs:?} {operator} {rhs:?}")]
    BinaryOperationOverflow {
        lhs: Value,
        rhs: Value,
        operator: NumericBinaryOperator,
    },

    #[error("non numeric value in sqrt {0:?}")]
    SqrtOnNonNumeric(Value),

    #[error("unsupported value by position function: from_str(from_str:?), sub_str(sub_str:?)")]
    UnSupportedValueByPositionFunction { from_str: Value, sub_str: Value },
}

#[derive(Debug, PartialEq, Eq, Serialize, Display)]
pub enum NumericBinaryOperator {
    #[strum(to_string = "+")]
    Add,
    #[strum(to_string = "-")]
    Subtract,
    #[strum(to_string = "*")]
    Multiply,
    #[strum(to_string = "/")]
    Divide,
    #[strum(to_string = "%")]
    Modulo,
}
