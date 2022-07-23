mod binary_op;
mod is_null;
mod nested;

pub mod aggregate;
pub mod extract;
pub mod function;

pub use nested::nested;

use {
    crate::{
        ast::{AstLiteral, BinaryOperator, DateTimeField, Expr},
        parse_sql::parse_expr,
        result::{Error, Result},
        translate::translate_expr,
    },
    aggregate::AggregateNode,
    bigdecimal::BigDecimal,
    function::FunctionNode,
};

#[derive(Clone)]
pub enum ExprNode {
    Expr(Expr),
    SqlExpr(String),
    Identifier(String),
    CompoundIdentifier(Vec<String>),
    BinaryOp {
        left: Box<ExprNode>,
        op: BinaryOperator,
        right: Box<ExprNode>,
    },
    Extract {
        field: DateTimeField,
        expr: Box<ExprNode>,
    },
    IsNull(Box<ExprNode>),
    IsNotNull(Box<ExprNode>),
    Nested(Box<ExprNode>),
    Function(Box<FunctionNode>),
    Aggregate(Box<AggregateNode>),
}

impl TryFrom<ExprNode> for Expr {
    type Error = Error;

    fn try_from(expr_node: ExprNode) -> Result<Self> {
        match expr_node {
            ExprNode::Expr(expr) => Ok(expr),
            ExprNode::SqlExpr(expr) => {
                let expr = parse_expr(expr)?;

                translate_expr(&expr)
            }
            ExprNode::Identifier(ident) => Ok(Expr::Identifier(ident)),
            ExprNode::CompoundIdentifier(idents) => Ok(Expr::CompoundIdentifier(idents)),
            ExprNode::BinaryOp { left, op, right } => {
                let left = Expr::try_from(*left).map(Box::new)?;
                let right = Expr::try_from(*right).map(Box::new)?;

                Ok(Expr::BinaryOp { left, op, right })
            }
            ExprNode::Extract { field, expr } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;
                Ok(Expr::Extract { field, expr })
            }
            ExprNode::IsNull(expr) => Expr::try_from(*expr).map(Box::new).map(Expr::IsNull),
            ExprNode::IsNotNull(expr) => Expr::try_from(*expr).map(Box::new).map(Expr::IsNotNull),
            ExprNode::Nested(expr) => Expr::try_from(*expr).map(Box::new).map(Expr::Nested),
            ExprNode::Function(func_expr) => Expr::try_from(*func_expr),
            ExprNode::Aggregate(aggr_expr) => Expr::try_from(*aggr_expr),
        }
    }
}

impl From<&str> for ExprNode {
    fn from(expr: &str) -> Self {
        ExprNode::SqlExpr(expr.to_owned())
    }
}

impl From<i64> for ExprNode {
    fn from(n: i64) -> Self {
        ExprNode::Expr(Expr::Literal(AstLiteral::Number(BigDecimal::from(n))))
    }
}

impl From<Expr> for ExprNode {
    fn from(expr: Expr) -> Self {
        ExprNode::Expr(expr)
    }
}

pub fn expr(value: &str) -> ExprNode {
    ExprNode::from(value)
}

pub fn col(value: &str) -> ExprNode {
    let idents = value.split('.').collect::<Vec<_>>();

    if idents.len() == 1 {
        ExprNode::Identifier(value.to_owned())
    } else {
        ExprNode::CompoundIdentifier(idents.into_iter().map(ToOwned::to_owned).collect())
    }
}

pub fn num(value: i64) -> ExprNode {
    ExprNode::from(value)
}

pub fn text(value: &str) -> ExprNode {
    ExprNode::Expr(Expr::Literal(AstLiteral::QuotedString(value.to_owned())))
}
