use {
    crate::{ast::DataType, data::Value},
    serde::Serialize,
    std::fmt::Debug,
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

    #[error("failed to parse date: {0}")]
    FailedToParseDate(String),

    #[error("failed to parse timestamp: {0}")]
    FailedToParseTimestamp(String),

    #[error("failed to parse time: {0}")]
    FailedToParseTime(String),

    #[error("failed to UUID: {0}")]
    FailedToParseUUID(String),

    #[error("add on non-numeric values: {0:?} + {1:?}")]
    AddOnNonNumeric(Value, Value),

    #[error("subtract on non-numeric values: {0:?} - {1:?}")]
    SubtractOnNonNumeric(Value, Value),

    #[error("multiply on non-numeric values: {0:?} * {1:?}")]
    MultiplyOnNonNumeric(Value, Value),

    #[error("divide on non-numeric values: {0:?} / {1:?}")]
    DivideOnNonNumeric(Value, Value),

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("modulo on non-numeric values: {0:?} % {1:?}")]
    ModuloOnNonNumeric(Value, Value),

    #[error("{0} type cannot be grouped by")]
    GroupByNotSupported(String),

    #[error("unary plus operation for non numeric value")]
    UnaryPlusOnNonNumeric,

    #[error("unary minus operation for non numeric value")]
    UnaryMinusOnNonNumeric,

    #[error("unreachable failure on parsing number")]
    UnreachableNumberParsing,

    #[error("conflict - unique constraint cannot be used for {0} type")]
    ConflictDataTypeOnUniqueConstraint(String),

    // Cast errors from value to value
    #[error("impossible cast")]
    ImpossibleCast,

    #[error("unimplemented cast")]
    UnimplementedCast,

    // Cast errors from literal to value
    #[error("literal cast failed from text to integer: {0}")]
    LiteralCastFromTextToIntegerFailed(String),

    #[error("literal cast failed from text to float: {0}")]
    LiteralCastToFloatFailed(String),

    #[error("literal cast failed to boolean: {0}")]
    LiteralCastToBooleanFailed(String),

    #[error("literal cast failed to date: {0}")]
    LiteralCastToDateFailed(String),

    #[error("literal cast failed to time: {0}")]
    LiteralCastToTimeFailed(String),

    #[error("unreachable literal cast from number to integer: {0}")]
    UnreachableLiteralCastFromNumberToInteger(String),

    #[error("unimplemented literal cast: {literal} as {data_type:?}")]
    UnimplementedLiteralCast {
        data_type: DataType,
        literal: String,
    },

    #[error("unreachable integer overflow: {0}")]
    UnreachableIntegerOverflow(String),

    #[error("operator doesn't exist: {0:?} LIKE {1:?}")]
    LikeOnNonString(Value, Value),

    #[error("operator doesn't exist: {0:?} ILIKE {1:?}")]
    ILikeOnNonString(Value, Value),

    #[error("big edian export not supported for {0} type")]
    BigEdianExportNotSupported(String),

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
}
