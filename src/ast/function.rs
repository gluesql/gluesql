use {
    super::Expr,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Function {
    Lower(Box<Expr>),
    Upper(Box<Expr>),
    Left { expr: Box<Expr>, size: Box<Expr> },
    Right { expr: Box<Expr>, size: Box<Expr> },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Aggregate {
    Count(Box<Expr>),
    Sum(Box<Expr>),
    Max(Box<Expr>),
    Min(Box<Expr>),
}
