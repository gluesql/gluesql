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

    #[error("failed to parse point: {0}")]
    FailedToParsePoint(String),

    #[error("failed to parse Decimal: {0}")]
    FailedToParseDecimal(String),

    #[error("failed to parse hex string: {0}")]
    FailedToParseHexString(String),

    #[error("failed to parse inet string: {0}")]
    FailedToParseInetString(String),

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

    #[error("GCD or LCM calculation overflowed on trying to get the absolute value of {0}")]
    GcdLcmOverflow(i64),

    #[error("LCM calculation resulted in a value out of the i64 range")]
    LcmResultOutOfRange,

    #[error("unary bit_not operation for non numeric value")]
    UnaryBitwiseNotOnNonNumeric,

    #[error("unary bit_not operation for non integer value")]
    UnaryBitwiseNotOnNonInteger,

    #[error("unreachable failure on parsing number")]
    UnreachableNumberParsing,

    #[error("unimplemented cast: {value:?} as {data_type}")]
    UnimplementedCast { value: Value, data_type: DataType },

    #[error("failed to cast from hex string to bytea: {0}")]
    CastFromHexToByteaFailed(String),

    #[error("function CONCAT requires at least 1 argument")]
    EmptyArgNotAllowedInConcat,

    // Cast errors from literal to value
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

    #[error("literal cast failed to UINT16: {0}")]
    LiteralCastToUint16Failed(String),

    #[error("literal cast failed to UNIT32: {0}")]
    LiteralCastToUint32Failed(String),

    #[error("literal cast failed to UNIT64: {0}")]
    LiteralCastToUint64Failed(String),

    #[error("literal cast failed to UNIT128: {0}")]
    LiteralCastToUint128Failed(String),

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

    #[error("failed to convert Value to u32: {0}")]
    I64ToU32ConversionFailure(String),
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
