use {crate::ast::DataType, serde::Serialize, std::fmt::Debug, thiserror::Error as ThisError};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum PlannerError {
    #[error("CTAS source table not found: {0}")]
    CtasSourceTableNotFound(String),

    #[error("cannot determine CTAS column type: {0}")]
    CtasColumnTypeUnknown(String),

    #[error("CTAS column {column} has type {found}, expected {expected}")]
    CtasColumnTypeMismatch {
        column: String,
        expected: DataType,
        found: DataType,
    },

    #[error("CTAS source has {found} columns, expected {expected}")]
    CtasColumnCountMismatch { expected: usize, found: usize },

    #[error("VALUES row {row} has {found} columns, expected {expected}")]
    CtasValuesLengthMismatch {
        row: usize,
        expected: usize,
        found: usize,
    },

    #[error("CTAS source and target must have the same schemaful or schemaless format")]
    CtasSourceFormatMismatch,

    #[error("cannot determine wildcard columns for CTAS source: {0}")]
    CtasWildcardColumnsUnknown(String),

    #[error("column not found: {0}")]
    CtasColumnNotFound(String),

    #[error("table alias not found: {0}")]
    CtasAliasNotFound(String),

    #[error("table alias has {found} column names, but the source has {expected} columns")]
    CtasAliasColumnsOverflow { expected: usize, found: usize },

    #[error("self-referencing foreign keys are not supported in CTAS: {0}")]
    UnsupportedSelfReferencingForeignKeyInCtas(String),

    #[error("malformed plan - unplanned CREATE TABLE columns have no source")]
    MalformedPlanCtasWithoutSource,

    #[error("malformed plan - VALUES has no rows")]
    MalformedPlanEmptyCtasValues,

    #[error("unplanned custom function: {0}")]
    UnplannedCustomFunction(String),

    #[error("storage access is not allowed in a custom function body")]
    StorageAccessInFunctionBody,

    #[error("custom function is not allowed in an argument default: {0}")]
    CustomFunctionInArgumentDefault(String),

    #[error("unsupported function: {0}")]
    UnsupportedFunction(String),

    #[error("malformed plan - custom function index is out of bounds: {0}")]
    MalformedPlanCustomFunctionIndex(usize),

    #[error(
        "function args.length not matching: {name}, expected: {expected_minimum} ~ {expected_maximum}, found: {found}"
    )]
    FunctionArgsLengthNotWithinRange {
        name: String,
        expected_minimum: usize,
        expected_maximum: usize,
        found: usize,
    },

    #[error("duplicate arg name: {0}")]
    DuplicateArgName(String),

    #[error("non-default argument should not follow the default argument")]
    NonDefaultArgumentFollowsDefaultArgument,

    /// Error that that omits when user projects common column name from multiple tables in `JOIN`
    /// situation.
    #[error("column reference {0} is ambiguous, please specify the table name")]
    ColumnReferenceAmbiguous(String),

    #[error(
        "SELECT * is not allowed for joins mixing schemaful and schemaless tables; use qualified wildcard or explicit columns"
    )]
    SchemalessMixedJoinWildcardProjection,

    #[error("specifying columns is not allowed for schemaless table INSERT; omit the column list")]
    SchemalessInsertWithExplicitColumns,

    #[error("unreachable")]
    Unreachable,
}
