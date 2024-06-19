use {
    crate::ast::{Aggregate, BinaryOperator, Expr, ToSql},
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

    #[error("function requires point value: {0}")]
    FunctionRequiresPointValue(String),

    #[error("function requires date or datetime value: {0}")]
    FunctionRequiresDateOrDateTimeValue(String),

    #[error("function requires one of string, list, map types: {0}")]
    FunctionRequiresStrOrListOrMapValue(String),

    #[error("identifier not found: {0}")]
    IdentifierNotFound(String),

    #[error("identifier not found: {table_alias}.{column_name}")]
    CompoundIdentifierNotFound {
        table_alias: String,
        column_name: String,
    },

    #[error("only boolean value is accepted: {0}")]
    BooleanTypeRequired(String),

    #[error("expr requires map or list value")]
    MapOrListTypeRequired,

    #[error("expr requires map value")]
    MapTypeRequired,

    #[error("expr requires list value")]
    ListTypeRequired,

    #[error("all elements in the list must be comparable to each other")]
    InvalidSortType,

    #[error("sort order must be either ASC or DESC")]
    InvalidSortOrder,

    #[error("map or string value required for json map conversion: {0}")]
    MapOrStringValueRequired(String),

    #[error("text literal required for json map conversion: {0}")]
    TextLiteralRequired(String),

    #[error("unsupported stateless expression: {}", .0.to_sql())]
    UnsupportedStatelessExpr(Expr),

    #[error("context is required for identifier evaluation: {}", .0.to_sql())]
    ContextRequiredForIdentEvaluation(Expr),

    #[error("unreachable empty aggregate value: {0:?}")]
    UnreachableEmptyAggregateValue(Aggregate),

    #[error("incompatible bit operation between {0} and {1}")]
    IncompatibleBitOperation(String, String),

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("negative substring length not allowed")]
    NegativeSubstrLenNotAllowed,

    #[error("subquery returns more than one row")]
    MoreThanOneRowReturned,

    #[error("subquery returns more than one column")]
    MoreThanOneColumnReturned,

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

    #[error("unsupported evaluate binary operation {} {} {}", .left, .op.to_sql(), .right)]
    UnsupportedBinaryOperation {
        left: String,
        op: BinaryOperator,
        right: String,
    },

    #[error("unsupported evaluate string unary plus: {0}")]
    UnsupportedUnaryPlus(String),

    #[error("unsupported evaluate string unary minus: {0}")]
    UnsupportedUnaryMinus(String),

    #[error("unsupported evaluate string unary factorial: {0}")]
    UnsupportedUnaryFactorial(String),

    #[error("incompatible bit operation ~{0}")]
    IncompatibleUnaryBitwiseNotOperation(String),

    #[error("unsupported custom function in subqueries")]
    UnsupportedCustomFunction,

    #[error(r#"The function "{function_name}" requires at least {required_minimum} argument(s), but {found} were provided."#)]
    FunctionRequiresMoreArguments {
        function_name: String,
        required_minimum: usize,
        found: usize,
    },

    #[error("function args.length not matching: {name}, expected: {expected_minimum} ~ {expected_maximum}, found: {found}")]
    FunctionArgsLengthNotWithinRange {
        name: String,
        expected_minimum: usize,
        expected_maximum: usize,
        found: usize,
    },

    #[error("unsupported function: {0}")]
    UnsupportedFunction(String),

    #[error("The provided arguments are non-comparable: {0}")]
    NonComparableArgumentError(String),

    #[error("function requires at least one argument: {0}")]
    FunctionRequiresAtLeastOneArgument(String),
}

fn error_serialize<S>(error: &chrono::format::ParseError, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let display = format!("{}", error);
    serializer.serialize_str(&display)
}
