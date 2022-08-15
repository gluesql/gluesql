mod binary_op;
mod is_null;
mod nested;
mod unary_op;

pub mod aggregate;
pub mod between;
pub mod cast;
pub mod extract;
pub mod function;
pub mod in_list;
pub mod in_subquery;

pub use nested::nested;

use {
    super::DataTypeNode,
    crate::{
        ast::{
            Aggregate, AstLiteral, BinaryOperator, DateTimeField, Expr, Function, Query,
            UnaryOperator,
        },
        ast_builder::QueryNode,
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
    Between {
        expr: Box<ExprNode>,
        negated: bool,
        low: Box<ExprNode>,
        high: Box<ExprNode>,
    },
    BinaryOp {
        left: Box<ExprNode>,
        op: BinaryOperator,
        right: Box<ExprNode>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<ExprNode>,
    },
    Extract {
        field: DateTimeField,
        expr: Box<ExprNode>,
    },
    IsNull(Box<ExprNode>),
    IsNotNull(Box<ExprNode>),
    InList {
        expr: Box<ExprNode>,
        list: Vec<ExprNode>,
        negated: bool,
    },
    InSubquery {
        expr: Box<ExprNode>,
        subquery: Box<QueryNode>,
        negated: bool,
    },
    Nested(Box<ExprNode>),
    Function(Box<FunctionNode>),
    Aggregate(Box<AggregateNode>),
    Cast {
        expr: Box<ExprNode>,
        data_type: DataTypeNode,
    },
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
            ExprNode::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;
                let low = Expr::try_from(*low).map(Box::new)?;
                let high = Expr::try_from(*high).map(Box::new)?;

                Ok(Expr::Between {
                    expr,
                    negated,
                    low,
                    high,
                })
            }
            ExprNode::BinaryOp { left, op, right } => {
                let left = Expr::try_from(*left).map(Box::new)?;
                let right = Expr::try_from(*right).map(Box::new)?;

                Ok(Expr::BinaryOp { left, op, right })
            }
            ExprNode::UnaryOp { op, expr } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;
                Ok(Expr::UnaryOp { op, expr })
            }
            ExprNode::Extract { field, expr } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;
                Ok(Expr::Extract { field, expr })
            }
            ExprNode::Cast { expr, data_type } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;
                let data_type = data_type.try_into()?;
                Ok(Expr::Cast { expr, data_type })
            }
            ExprNode::IsNull(expr) => Expr::try_from(*expr).map(Box::new).map(Expr::IsNull),
            ExprNode::IsNotNull(expr) => Expr::try_from(*expr).map(Box::new).map(Expr::IsNotNull),
            ExprNode::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;
                let list = list
                    .into_iter()
                    .map(Expr::try_from)
                    .collect::<Result<Vec<_>>>()?;

                Ok(Expr::InList {
                    expr,
                    list,
                    negated,
                })
            }
            ExprNode::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;
                let subquery = Query::try_from(*subquery).map(Box::new)?;

                Ok(Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                })
            }
            ExprNode::Nested(expr) => Expr::try_from(*expr).map(Box::new).map(Expr::Nested),
            ExprNode::Function(func_expr) => Function::try_from(*func_expr)
                .map(Box::new)
                .map(Expr::Function),
            ExprNode::Aggregate(aggr_expr) => Aggregate::try_from(*aggr_expr)
                .map(Box::new)
                .map(Expr::Aggregate),
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
