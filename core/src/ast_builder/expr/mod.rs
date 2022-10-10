mod binary_op;
mod exists;
mod is_null;
mod like;
mod nested;
mod unary_op;

pub mod aggregate;
pub mod between;
pub mod extract;
pub mod function;
pub mod in_list;

pub use exists::{exists, not_exists};
pub use nested::nested;

use {
    crate::{
        ast::{
            Aggregate, AstLiteral, BinaryOperator, DateTimeField, Expr, Function, Query,
            UnaryOperator,
        },
        ast_builder::QueryNode,
        parse_sql::{parse_comma_separated_exprs, parse_expr, parse_query},
        prelude::DataType,
        result::{Error, Result},
        translate::{translate_expr, translate_query},
    },
    aggregate::AggregateNode,
    bigdecimal::BigDecimal,
    function::FunctionNode,
    in_list::InListNode,
};

#[derive(Clone)]
pub enum ExprNode {
    Expr(Expr),
    SqlExpr(String),
    Identifier(String),
    CompoundIdentifier {
        alias: String,
        ident: String,
    },
    Between {
        expr: Box<ExprNode>,
        negated: bool,
        low: Box<ExprNode>,
        high: Box<ExprNode>,
    },
    Like {
        expr: Box<ExprNode>,
        negated: bool,
        pattern: Box<ExprNode>,
    },
    ILike {
        expr: Box<ExprNode>,
        negated: bool,
        pattern: Box<ExprNode>,
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
        list: Box<InListNode>,
        negated: bool,
    },
    Nested(Box<ExprNode>),
    Function(Box<FunctionNode>),
    Aggregate(Box<AggregateNode>),
    Exists {
        subquery: Box<QueryNode>,
        negated: bool,
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
            ExprNode::CompoundIdentifier { alias, ident } => {
                Ok(Expr::CompoundIdentifier { alias, ident })
            }
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
            ExprNode::Like {
                expr,
                negated,
                pattern,
            } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;
                let pattern = Expr::try_from(*pattern).map(Box::new)?;

                Ok(Expr::Like {
                    expr,
                    negated,
                    pattern,
                })
            }
            ExprNode::ILike {
                expr,
                negated,
                pattern,
            } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;
                let pattern = Expr::try_from(*pattern).map(Box::new)?;

                Ok(Expr::ILike {
                    expr,
                    negated,
                    pattern,
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
            ExprNode::IsNull(expr) => Expr::try_from(*expr).map(Box::new).map(Expr::IsNull),
            ExprNode::IsNotNull(expr) => Expr::try_from(*expr).map(Box::new).map(Expr::IsNotNull),
            ExprNode::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Expr::try_from(*expr).map(Box::new)?;

                match *list {
                    InListNode::InList(list) => {
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
                    InListNode::Query(subquery) => {
                        let subquery = Query::try_from(*subquery).map(Box::new)?;
                        Ok(Expr::InSubquery {
                            expr,
                            subquery,
                            negated,
                        })
                    }
                    InListNode::Text(value) => {
                        let subquery = parse_query(value.clone())
                            .and_then(|item| translate_query(&item))
                            .map(Box::new);

                        if let Ok(subquery) = subquery {
                            return Ok(Expr::InSubquery {
                                expr,
                                subquery,
                                negated,
                            });
                        }

                        parse_comma_separated_exprs(&*value)?
                            .iter()
                            .map(translate_expr)
                            .collect::<Result<Vec<_>>>()
                            .map(|list| Expr::InList {
                                expr,
                                list,
                                negated,
                            })
                    }
                }
            }
            ExprNode::Nested(expr) => Expr::try_from(*expr).map(Box::new).map(Expr::Nested),
            ExprNode::Function(func_expr) => Function::try_from(*func_expr)
                .map(Box::new)
                .map(Expr::Function),
            ExprNode::Aggregate(aggr_expr) => Aggregate::try_from(*aggr_expr)
                .map(Box::new)
                .map(Expr::Aggregate),
            ExprNode::Exists { subquery, negated } => Query::try_from(*subquery)
                .map(Box::new)
                .map(|subquery| Expr::Exists { subquery, negated }),
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

    match idents.as_slice() {
        [alias, ident] => ExprNode::CompoundIdentifier {
            alias: alias.to_string(),
            ident: ident.to_string(),
        },
        _ => ExprNode::Identifier(value.to_owned()),
    }
}

pub fn num(value: i64) -> ExprNode {
    ExprNode::from(value)
}

pub fn text(value: &str) -> ExprNode {
    ExprNode::Expr(Expr::Literal(AstLiteral::QuotedString(value.to_owned())))
}

pub fn date(date: &str) -> ExprNode {
    ExprNode::Expr(Expr::TypedString {
        data_type: DataType::Date,
        value: date.to_owned(),
    })
}

pub fn timestamp(timestamp: &str) -> ExprNode {
    ExprNode::Expr(Expr::TypedString {
        data_type: DataType::Timestamp,
        value: timestamp.to_owned(),
    })
}

pub fn time(time: &str) -> ExprNode {
    ExprNode::Expr(Expr::TypedString {
        data_type: DataType::Time,
        value: time.to_owned(),
    })
}
