use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum TranslateError {
    #[error("unimplemented - select query without table is not supported")]
    LackOfTable,

    #[error("unimplemented - select on two or more than tables are not supported")]
    TooManyTables,

    #[error("unimplemented - composite index is not supported")]
    CompositeIndexNotSupported,

    #[error("unimplemented - join on update not supported")]
    JoinOnUpdateNotSupported,

    #[error("unimplemented - compound identifier on update not supported: {0}")]
    CompoundIdentOnUpdateNotSupported(String),

    #[error("too many params in drop index")]
    TooManyParamsInDropIndex,

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

    #[error("function args.length not matching: {name}, expected: {expected_minimum} ~ {expected_maximum}, found: {found}")]
    FunctionArgsLengthNotWithinRange {
        name: String,
        expected_minimum: usize,
        expected_maximum: usize,
        found: usize,
    },

    #[error("named function arg is not supported")]
    NamedFunctionArgNotSupported,

    #[error("wildcard function arg is not accepted")]
    WildcardFunctionArgNotAccepted,

    #[error("qualified wildcard is not supported - COUNT({0})")]
    QualifiedWildcardInCountNotSupported(String),

    #[error("order by - NULLS (FIRST | LAST) is not supported")]
    OrderByNullsFirstOrLastNotSupported,

    #[error("unsupported function: {0}")]
    UnsupportedFunction(String),

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

    #[error("unsupported ast literal: {0}")]
    UnsupportedAstLiteral(String),

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
    UnsupportedJoinConstraint(String),

    #[error("unsupported join operator: {0}")]
    UnsupportedJoinOperator(String),

    #[error("unsupported column option: {0}")]
    UnsupportedColumnOption(String),

    #[error("unsupported alter table operation: {0}")]
    UnsupportedAlterTableOperation(String),

    #[error("unsupported table factor: {0}")]
    UnsupportedTableFactor(String),
}
