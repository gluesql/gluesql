use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum ValueError {
    #[error("literal: {literal} is incompatible with data type: {data_type}")]
    IncompatibleLiteralForDataType { data_type: String, literal: String },

    #[error("failed to parse number")]
    FailedToParseNumber,

    #[error("add on non numeric value")]
    AddOnNonNumeric,

    #[error("subtract on non numeric value")]
    SubtractOnNonNumeric,

    #[error("multiply on non numeric value")]
    MultiplyOnNonNumeric,

    #[error("divide on non numeric value")]
    DivideOnNonNumeric,

    #[error("null value on not null field")]
    NullValueOnNotNullField,

    #[error("floating numbers cannot be grouped by")]
    FloatCannotBeGroupedBy,

    #[error("unary plus operation for non numeric value")]
    UnaryPlusOnNonNumeric,

    #[error("unary minus operation for non numeric value")]
    UnaryMinusOnNonNumeric,

    #[error("unreachable failure on parsing number")]
    UnreachableNumberParsing,

    #[error("floating columns cannot be set to unique constraint")]
    ConflictOnFloatWithUniqueConstraint,

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

    #[error("unreachable literal cast from number to integer: {0}")]
    UnreachableLiteralCastFromNumberToInteger(String),

    #[error("unimplemented literal cast: {literal} as {data_type}")]
    UnimplementedLiteralCast { data_type: String, literal: String },
}
