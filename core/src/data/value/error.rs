use {
    crate::{
        ast::{DataType, DateTimeField},
        data::Value,
    },
    serde::Serialize,
    std::fmt::Debug,
    strum_macros::Display,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum ValueError {
    #[error("incompatible data type, data type: {data_type:#?}, value: {value:#?}")]
    IncompatibleDataType { data_type: DataType, value: Value },

    #[error("null value on not null field")]
    NullValueOnNotNullField,

    #[error("failed to convert Float to Decimal: {0}")]
    FloatToDecimalConversionFailure(f64),

    #[error("failed to UUID: {0}")]
    FailedToParseUUID(String),

    #[error("failed to parse point: {0}")]
    FailedToParsePoint(String),

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

    #[error("unary bit_not operation for non numeric value")]
    UnaryBitwiseNotOnNonNumeric,

    #[error("unary bit_not operation for non integer value")]
    UnaryBitwiseNotOnNonInteger,

    #[error("unimplemented cast: {value:?} as {data_type}")]
    UnimplementedCast { value: Value, data_type: DataType },

    #[error("failed to cast from hex string to bytea: {0}")]
    CastFromHexToByteaFailed(String),

    #[error("unreachable integer overflow: {0}")]
    UnreachableIntegerOverflow(String),

    #[error("operator doesn't exist: {base:?} {case} {pattern:?}", case = if *case_sensitive { "LIKE" } else { "ILIKE" })]
    LikeOnNonString {
        base: Value,
        pattern: Value,
        case_sensitive: bool,
    },

    #[error("extract format not matched: {value:?} FROM {field:?})")]
    ExtractFormatNotMatched { value: Value, field: DateTimeField },

    #[error("big endian export not supported for {0} type")]
    BigEndianExportNotSupported(String),

    #[error("invalid json string: {0}")]
    InvalidJsonString(String),

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

    #[error("non-string parameter in position: {} IN {}", String::from(.from), String::from(.sub))]
    NonStringParameterInPosition { from: Value, sub: Value },

    #[error("non-string parameter in find idx: {}, {}", String::from(.sub), String::from(.from))]
    NonStringParameterInFindIdx { sub: Value, from: Value },

    #[error("non positive offset in find idx: {0}")]
    NonPositiveIntegerOffsetInFindIdx(String),

    #[error("failed to convert Value to Expr")]
    ValueToExprConversionFailure,
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
    #[strum(to_string = "&")]
    BitwiseAnd,
    #[strum(to_string = "<<")]
    BitwiseShiftLeft,
    #[strum(to_string = ">>")]
    BitwiseShiftRight,
}
