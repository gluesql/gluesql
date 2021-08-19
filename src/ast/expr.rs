use core::fmt;
use std::fmt::Formatter;
use {
    super::{Aggregate, AstLiteral, BinaryOperator, DataType, Function, Query, UnaryOperator},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Expr {
    Identifier(String),
    Wildcard,
    QualifiedWildcard(Vec<String>),
    CompoundIdentifier(Vec<String>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    Nested(Box<Expr>),
    Literal(AstLiteral),
    TypedString {
        data_type: DataType,
        value: String,
    },
    Function(Box<Function>),
    Aggregate(Box<Aggregate>),
    Exists(Box<Query>),
    Subquery(Box<Query>),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
