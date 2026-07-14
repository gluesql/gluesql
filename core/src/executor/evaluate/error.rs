use {
    crate::{
        ast::{BinaryOperator, DataType, ToSql},
        plan::AggregatePlan,
    },
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum EvaluateError {
    #[error("{0}")]
    FormatParseError(String),

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("function requires string value: {0}")]
    FunctionRequiresStringValue(String),

    #[error("function requires integer or string value: {0}")]
    FunctionRequiresIntegerOrStringValue(String),

    #[error("arrow base requires MAP or LIST types")]
    ArrowBaseRequiresMapOrList,

    #[error("arrow selector requires integer or string value: {0}")]
    ArrowSelectorRequiresIntegerOrString(String),

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

    #[error("subquery is not allowed in stateless expression")]
    SubqueryNotAllowedInStatelessExpr,

    #[error("IN (subquery) is not allowed in stateless expression")]
    InSubqueryNotAllowedInStatelessExpr,

    #[error("EXISTS (subquery) is not allowed in stateless expression")]
    ExistsSubqueryNotAllowedInStatelessExpr,

    #[error("row context is required for identifier evaluation: {0}")]
    IdentifierRequiresRowContext(String),

    #[error("row context is required for compound identifier evaluation: {alias}.{ident}")]
    CompoundIdentifierRequiresRowContext { alias: String, ident: String },

    #[error("aggregate slot value missing: {0:?}")]
    AggregateSlotValueMissing(Box<AggregatePlan>),

    #[error("aggregate expression requires planner binding: {0:?}")]
    UnplannedAggregate(Box<AggregatePlan>),

    #[error("filter context is required for aggregate function: {0:?}")]
    FilterContextRequiredForAggregate(Box<AggregatePlan>),

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

    #[error("IN (subquery) must return one column")]
    InSubqueryMustReturnOneColumn,

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

    #[error("unary factorial requires numeric literal: {0}")]
    UnaryFactorialRequiresNumericLiteral(String),

    #[error("unary bitwise-not requires integer literal: {0}")]
    UnaryBitwiseNotRequiresIntegerLiteral(String),

    #[error("operator doesn't exist: {base} {case} {pattern}", case = if *case_sensitive { "LIKE" } else { "ILIKE" })]
    LikeOnNonStringLiteral {
        base: String,
        pattern: String,
        case_sensitive: bool,
    },

    #[error("unsupported custom function in subqueries")]
    UnsupportedCustomFunction,

    #[error(r#"The function "{function_name}" requires at least {required_minimum} argument(s), but {found} were provided."#)]
    FunctionRequiresMoreArguments {
        function_name: String,
        required_minimum: usize,
        found: usize,
    },

    #[error(
        "function args.length not matching: {name}, expected: {expected_minimum} ~ {expected_maximum}, found: {found}"
    )]
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

    #[error("function CONCAT requires at least 1 argument")]
    EmptyArgNotAllowedInConcat,

    #[error("LCM calculation resulted in a value out of the i64 range")]
    LcmResultOutOfRange,

    #[error("GCD or LCM calculation overflowed on trying to get the absolute value of {0}")]
    GcdLcmOverflow(i64),

    #[error("failed to convert Value to u32: {0}")]
    I64ToU32ConversionFailure(String),

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
