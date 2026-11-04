use {serde::Serialize, std::fmt::Debug, thiserror::Error as ThisError};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum PlannerError {
    #[error("CREATE INDEX target table does not exist: {0}")]
    CreateIndexTableNotFound(String),

    #[error("unsupported index expression")]
    UnsupportedIndexExpr,

    #[error("index expression must reference at least one column")]
    IndexExprRequiresColumnReference,

    #[error("CTAS source table does not exist: {0}")]
    CtasSourceTableNotFound(String),

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
