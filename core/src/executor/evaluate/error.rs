use {
    crate::ast::{Aggregate, Expr},
    serde::{Serialize, Serializer},
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum EvaluateError {
    #[error(transparent)]
    #[serde(serialize_with = "error_serialize")]
    FormatParseError(#[from] chrono::format::ParseError),

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("function requires string value: {0}")]
    FunctionRequiresStringValue(String),

    #[error("function requires integer value: {0}")]
    FunctionRequiresIntegerValue(String),

    #[error("function requires float or integer value: {0}")]
    FunctionRequiresFloatOrIntegerValue(String),

    #[error("function requires usize value: {0}")]
    FunctionRequiresUSizeValue(String),

    #[error("function requires float value: {0}")]
    FunctionRequiresFloatValue(String),

    #[error("extract format does not support value: {0}")]
    ExtractFormatNotMatched(String),

    #[error("function requires map value: {0}")]
    FunctionRequiresMapValue(String),

    #[error("value not found: {0}")]
    ValueNotFound(String),

    #[error("only boolean value is accepted: {0}")]
    BooleanTypeRequired(String),

    #[error("expr requires map or list value")]
    MapOrListTypeRequired,

    #[error("expr requires list value")]
    ListTypeRequired,

    #[error("map or string value required for json map conversion: {0}")]
    MapOrStringValueRequired(String),

    #[error("text literal required for json map conversion: {0}")]
    TextLiteralRequired(String),

    #[error("unsupported stateless expression: {0:#?}")]
    UnsupportedStatelessExpr(Expr),

    #[error("unreachable empty context")]
    UnreachableEmptyContext,

    #[error("unreachable empty aggregate value: {0:?}")]
    UnreachableEmptyAggregateValue(Aggregate),

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("negative substring length not allowed")]
    NegativeSubstrLenNotAllowed,

    #[error("subquery returns more than one row")]
    MoreThanOneRowReturned,

    #[error("schemaless projection is not allowed for IN (subquery)")]
    SchemalessProjectionForInSubQuery,

    #[error("schemaless projection is not allowed for subquery")]
    SchemalessProjectionForSubQuery,

    #[error("format function does not support following data_type: {0}")]
    UnsupportedExprForFormatFunction(String),

    #[error("support single character only")]
    AsciiFunctionRequiresSingleCharacterValue,

    #[error("non-ascii character not allowed")]
    NonAsciiCharacterNotAllowed,

    #[error("function requires integer value in range")]
    ChrFunctionRequiresIntegerValueInRange0To255,
}

fn error_serialize<S>(error: &chrono::format::ParseError, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let display = format!("{}", error);
    serializer.serialize_str(&display)
}
