use {serde::Serialize, std::fmt::Debug, strum_macros::Display, thiserror::Error};

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTableOption {
    #[strum(to_string = "TEMPORARY clause")]
    Temporary,

    #[strum(to_string = "LIKE clause")]
    Like,

    #[strum(to_string = "CLONE clause")]
    Clone,
}

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOption {
    #[strum(to_string = "RETURNING clause")]
    Returning,

    #[strum(to_string = "ON CONFLICT clause")]
    OnConflict,

    #[strum(to_string = "table alias")]
    TableAlias,

    #[strum(to_string = "PARTITION clause")]
    Partition,

    #[strum(to_string = "OVERWRITE clause")]
    Overwrite,

    #[strum(to_string = "TABLE keyword")]
    TableKeyword,
}

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOption {
    #[strum(to_string = "FROM clause")]
    From,

    #[strum(to_string = "RETURNING clause")]
    Returning,
}

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOption {
    #[strum(to_string = "USING clause")]
    Using,

    #[strum(to_string = "RETURNING clause")]
    Returning,

    #[strum(to_string = "ORDER BY clause")]
    OrderBy,

    #[strum(to_string = "LIMIT clause")]
    Limit,
}

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOption {
    #[strum(to_string = "WITH clause")]
    With,

    #[strum(to_string = "FETCH clause")]
    Fetch,

    #[strum(to_string = "LOCK clause")]
    Lock,
}

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectOption {
    #[strum(to_string = "INTO clause")]
    Into,

    #[strum(to_string = "WINDOW clause")]
    Window,
}

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinConstraintReason {
    #[strum(to_string = "USING")]
    Using,

    #[strum(to_string = "NATURAL")]
    Natural,
}

/// Serializes a `strum::Display`-backed enum as its display string, keeping
/// JSON output identical to the pre-refactor `&'static str`/`String` fields
/// without duplicating each variant's text in a second `#[serde(rename)]`.
macro_rules! serialize_via_display {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl Serialize for $ty {
                fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    serializer.collect_str(self)
                }
            }
        )+
    };
}

serialize_via_display!(
    CreateTableOption,
    InsertOption,
    UpdateOption,
    DeleteOption,
    QueryOption,
    SelectOption,
    JoinConstraintReason,
);

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum TranslateError {
    #[error("unimplemented - select on two or more than tables are not supported")]
    TooManyTables,

    #[error("unimplemented - SELECT DISTINCT ON is not supported")]
    SelectDistinctOnNotSupported,

    #[error("unimplemented - composite index is not supported")]
    CompositeIndexNotSupported,

    #[error("unimplemented - join on update not supported")]
    JoinOnUpdateNotSupported,

    #[error("unimplemented - compound identifier on update not supported: {0}")]
    CompoundIdentOnUpdateNotSupported(String),

    #[error("unimplemented - tuple assigment on update is not supported: {0}")]
    TupleAssignmentOnUpdateNotSupported(String),

    #[error("too many params in drop index")]
    TooManyParamsInDropIndex,

    #[error("invalid params in drop index, expected: table_name.index_name")]
    InvalidParamsInDropIndex,

    #[error("function args.length not matching: {name}, expected: {expected}, found: {found}")]
    FunctionArgsLengthNotMatching {
        name: String,
        expected: usize,
        found: usize,
    },

    #[error("function {name} requires at least {expected_minimum} argument(s), found: {found}")]
    FunctionArgsLengthNotMatchingMin {
        name: String,
        expected_minimum: usize,
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

    #[error("named function arg is not supported")]
    NamedFunctionArgNotSupported,

    #[error("unnamed function arg is not supported")]
    UnNamedFunctionArgNotSupported,

    #[error("subquery function arg is not supported")]
    UnreachableSubqueryFunctionArgNotSupported,

    #[error("INSERT INTO {0} DEFAULT VALUES is not supported")]
    DefaultValuesOnInsertNotSupported(String),

    #[error("empty function body is not supported")]
    UnsupportedEmptyFunctionBody,

    #[error("unsupported unnamed index")]
    UnsupportedUnnamedIndex,

    #[error("unsupported INSERT option: {0}")]
    UnsupportedInsertOption(InsertOption),

    #[error("unsupported CREATE TABLE option: {0}")]
    UnsupportedCreateTableOption(CreateTableOption),

    #[error("unsupported UPDATE option: {0}")]
    UnsupportedUpdateOption(UpdateOption),

    #[error("unsupported DELETE option: {0}")]
    UnsupportedDeleteOption(DeleteOption),

    #[error("unsupported query option: {0}")]
    UnsupportedQueryOption(QueryOption),

    #[error("unsupported SELECT option: {0}")]
    UnsupportedSelectOption(SelectOption),

    #[error(
        "unsupported trim chars: expected: `TRIM((BOTH | LEADING | TRAILING) <text> FROM <expr>)`, got: `TRIM(<expr> [<chars>, ..])` syntax"
    )]
    UnsupportedTrimChars,

    #[error("unsupported CAST format: {0}")]
    UnsupportedCastFormat(String),

    #[error("TRY_CAST(..) is not supported")]
    TryCastNotSupported,

    #[error("SAFE_CAST(..) is not supported")]
    SafeCastNotSupported,

    #[error(
        "unsupported multiple alter table operations, expected: `ALTER TABLE <table> <operation>`, got: `ALTER TABLE <table> <operation>, <operation>, ..`"
    )]
    UnsupportedMultipleAlterTableOperations,

    #[error("unreachable empty alter table operation")]
    UnreachableEmptyAlterTableOperation,

    #[error("unsupported `GROUP BY (ALL)`")]
    UnsupportedGroupByAll,

    #[error("wildcard function arg is not accepted")]
    WildcardFunctionArgNotAccepted,

    #[error("qualified wildcard is not supported - COUNT({0})")]
    QualifiedWildcardInCountNotSupported(String),

    #[error("order by - NULLS (FIRST | LAST) is not supported")]
    OrderByNullsFirstOrLastNotSupported,

    #[error("unsupported SHOW VARIABLE keyword: {0}")]
    UnsupportedShowVariableKeyword(String),

    #[error("unsupported SHOW VARIABLE statement: {0}")]
    UnsupportedShowVariableStatement(String),

    #[error("unsupported statement: {0}")]
    UnsupportedStatement(String),

    #[error("unsupported expr: {0}")]
    UnsupportedExpr(String),

    #[error("unsupported data type: {0}")]
    UnsupportedDataType(String),

    #[error("unsupported datetime field: {0}")]
    UnsupportedDateTimeField(String),

    #[error("unsupported literal: {0}")]
    UnsupportedLiteral(String),

    #[error("failed to decode hex string: {0}")]
    FailedToDecodeHexString(String),

    #[error("unreachable unary operator: {0}")]
    UnreachableUnaryOperator(String),

    #[error("unreachable empty ident")]
    UnreachableEmptyIdent,

    #[error("unsupported binary operator: {0}")]
    UnsupportedBinaryOperator(String),

    #[error("unsupported query set expr: {0}")]
    UnsupportedQuerySetExpr(String),

    #[error("unsupported query table factor: {0}")]
    UnsupportedQueryTableFactor(String),

    #[error("unsupported join constraint: {0}")]
    UnsupportedJoinConstraint(JoinConstraintReason),

    #[error("unsupported join operator: {0}")]
    UnsupportedJoinOperator(String),

    #[error("unsupported column option: {0}")]
    UnsupportedColumnOption(String),

    #[error("unsupported alter table operation: {0}")]
    UnsupportedAlterTableOperation(String),

    #[error("unsupported table factor: {0}")]
    UnsupportedTableFactor(String),

    #[error("Every derived table must have its own alias")]
    LackOfAlias,

    #[error("Series should have size")]
    LackOfArgs,

    #[error("unreachable empty object")]
    UnreachableEmptyObject,

    #[error("unreachable empty table")]
    UnreachableEmptyTable,

    #[error("unreachable - FROM cannot be ommitted in DELETE statement")]
    UnreachableOmittingFromInDelete,

    #[error("unimplemented - compound object is supported: {0}")]
    CompoundObjectNotSupported(String),

    #[error("cannot create index with reserved name: {0}")]
    ReservedIndexName(String),

    #[error("cannot drop primary index")]
    CannotDropPrimary,

    #[error("unreachable - empty columns")]
    UnreachableForeignKeyColumns(String),

    #[error("unsupported constraint: {0}")]
    UnsupportedConstraint(String),

    #[error("parameter index {index} is out of range (total parameters: {len})")]
    ParameterIndexOutOfRange { index: usize, len: usize },

    #[error("invalid parameter placeholder: {placeholder}")]
    InvalidPlaceholder { placeholder: String },
}
