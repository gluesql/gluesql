use {serde::Serialize, std::fmt::Debug, strum_macros::Display, thiserror::Error};

/// `CREATE TABLE` clauses that `GlueSQL` does not support yet.
///
/// Carried by [`TranslateError::UnsupportedCreateTableOption`] so callers
/// can match exhaustively on every currently-rejected clause instead of
/// inspecting a free-form string.
#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTableOption {
    /// `CREATE TEMPORARY TABLE ...`
    #[strum(to_string = "TEMPORARY clause")]
    Temporary,

    /// `CREATE TABLE ... LIKE <table>`
    #[strum(to_string = "LIKE clause")]
    Like,

    /// `CREATE TABLE ... CLONE <table>`
    #[strum(to_string = "CLONE clause")]
    CloneTable,
}

/// `INSERT` clauses that `GlueSQL` does not support yet.
///
/// Carried by [`TranslateError::UnsupportedInsertOption`] so callers can
/// match exhaustively on every currently-rejected clause instead of
/// inspecting a free-form string.
#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOption {
    /// `INSERT ... RETURNING ...`
    #[strum(to_string = "RETURNING clause")]
    Returning,

    /// `INSERT ... ON CONFLICT ...`
    #[strum(to_string = "ON CONFLICT clause")]
    OnConflict,

    /// `INSERT INTO <table> AS <alias> ...`
    #[strum(to_string = "table alias")]
    TableAlias,

    /// `INSERT INTO <table> PARTITION (...) ...`
    #[strum(to_string = "PARTITION clause")]
    Partition,

    /// `INSERT OVERWRITE TABLE <table> ...`
    #[strum(to_string = "OVERWRITE clause")]
    Overwrite,

    /// `INSERT TABLE <table> ...` (the `TABLE` keyword form)
    #[strum(to_string = "TABLE keyword")]
    TableKeyword,
}

/// `UPDATE` clauses that `GlueSQL` does not support yet.
///
/// Carried by [`TranslateError::UnsupportedUpdateOption`] so callers can
/// match exhaustively on every currently-rejected clause instead of
/// inspecting a free-form string.
#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOption {
    /// `UPDATE ... FROM ...`
    #[strum(to_string = "FROM clause")]
    From,

    /// `UPDATE ... RETURNING ...`
    #[strum(to_string = "RETURNING clause")]
    Returning,
}

/// `DELETE` clauses that `GlueSQL` does not support yet.
///
/// Carried by [`TranslateError::UnsupportedDeleteOption`] so callers can
/// match exhaustively on every currently-rejected clause instead of
/// inspecting a free-form string.
#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOption {
    /// `DELETE ... USING ...`
    #[strum(to_string = "USING clause")]
    Using,

    /// `DELETE ... RETURNING ...`
    #[strum(to_string = "RETURNING clause")]
    Returning,

    /// `DELETE ... ORDER BY ...`
    #[strum(to_string = "ORDER BY clause")]
    OrderBy,

    /// `DELETE ... LIMIT ...`
    #[strum(to_string = "LIMIT clause")]
    Limit,
}

/// Query-level (`WITH`/`FETCH`/locking) clauses that `GlueSQL` does not
/// support yet.
///
/// Carried by [`TranslateError::UnsupportedQueryOption`] so callers can
/// match exhaustively on every currently-rejected clause instead of
/// inspecting a free-form string.
#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOption {
    /// `WITH <cte> ... SELECT ...`
    #[strum(to_string = "WITH clause")]
    With,

    /// `SELECT ... FETCH FIRST ...`
    #[strum(to_string = "FETCH clause")]
    Fetch,

    /// `SELECT ... FOR UPDATE` / other row-locking clauses
    #[strum(to_string = "LOCK clause")]
    Lock,
}

/// `SELECT` clauses that `GlueSQL` does not support yet.
///
/// Carried by [`TranslateError::UnsupportedSelectOption`] so callers can
/// match exhaustively on every currently-rejected clause instead of
/// inspecting a free-form string.
#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectOption {
    /// `SELECT ... INTO <table> ...`
    #[strum(to_string = "INTO clause")]
    Into,

    /// `SELECT ... WINDOW <name> AS (...)`
    #[strum(to_string = "WINDOW clause")]
    Window,
}

/// `JOIN` constraint forms that `GlueSQL` does not support yet.
///
/// Carried by [`TranslateError::UnsupportedJoinConstraint`] so callers can
/// match exhaustively on every currently-rejected form instead of
/// inspecting a free-form string.
#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinConstraintReason {
    /// `... JOIN ... USING (...)`
    #[strum(to_string = "USING")]
    Using,

    /// `... NATURAL JOIN ...`
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

    #[error("named window is not supported")]
    NamedWindowNotSupported,

    #[error("window frame clause is not supported")]
    WindowFrameNotSupported,

    #[error("DISTINCT is not supported inside a window aggregate")]
    DistinctNotSupportedInWindowFunction,

    #[error("unsupported window function: {0}")]
    UnsupportedWindowFunction(String),

    #[error("window offset must be a non-negative integer literal: {0}")]
    InvalidWindowOffset(String),
}
