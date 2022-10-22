mod binary_op;
mod case;
mod exists;
mod is_null;
mod like;
mod nested;
mod unary_op;

pub mod aggregate;
pub mod between;
pub mod function;
pub mod in_list;

pub use case::case;
pub use exists::{exists, not_exists};
pub use nested::nested;
pub use unary_op::{factorial, minus, not, plus};

use {
    crate::{
        ast::{Aggregate, AstLiteral, BinaryOperator, Expr, Function, Query, UnaryOperator},
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
    std::borrow::Cow,
};

#[derive(Clone)]
pub enum ExprNode<'a> {
    Expr(Cow<'a, Expr>),
    SqlExpr(String),
    Identifier(String),
    CompoundIdentifier {
        alias: String,
        ident: String,
    },
    Between {
        expr: Box<ExprNode<'a>>,
        negated: bool,
        low: Box<ExprNode<'a>>,
        high: Box<ExprNode<'a>>,
    },
    Like {
        expr: Box<ExprNode<'a>>,
        negated: bool,
        pattern: Box<ExprNode<'a>>,
    },
    ILike {
        expr: Box<ExprNode<'a>>,
        negated: bool,
        pattern: Box<ExprNode<'a>>,
    },
    BinaryOp {
        left: Box<ExprNode<'a>>,
        op: BinaryOperator,
        right: Box<ExprNode<'a>>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<ExprNode<'a>>,
    },
    IsNull(Box<ExprNode<'a>>),
    IsNotNull(Box<ExprNode<'a>>),
    InList {
        expr: Box<ExprNode<'a>>,
        list: Box<InListNode<'a>>,
        negated: bool,
    },
    Nested(Box<ExprNode<'a>>),
    Function(Box<FunctionNode<'a>>),
    Aggregate(Box<AggregateNode<'a>>),
    Exists {
        subquery: Box<QueryNode<'a>>,
        negated: bool,
    },
    Subquery(Box<QueryNode<'a>>),
    Case {
        operand: Option<Box<ExprNode<'a>>>,
        when_then: Vec<(ExprNode<'a>, ExprNode<'a>)>,
        else_result: Option<Box<ExprNode<'a>>>,
    },
}

impl<'a> TryFrom<ExprNode<'a>> for Expr {
    type Error = Error;

    fn try_from(expr_node: ExprNode<'a>) -> Result<Self> {
        match expr_node {
            ExprNode::Expr(expr) => Ok(expr.into_owned()),
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
            ExprNode::Subquery(subquery) => {
                Query::try_from(*subquery).map(Box::new).map(Expr::Subquery)
            }
            ExprNode::Case {
                operand,
                when_then,
                else_result,
            } => {
                let operand = operand
                    .map(|expr| Expr::try_from(*expr))
                    .transpose()?
                    .map(Box::new);
                let when_then = when_then
                    .into_iter()
                    .map(|(when, then)| {
                        let when = Expr::try_from(when)?;
                        let then = Expr::try_from(then)?;
                        Ok((when, then))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let else_result = else_result
                    .map(|expr| Expr::try_from(*expr))
                    .transpose()?
                    .map(Box::new);
                Ok(Expr::Case {
                    operand,
                    when_then,
                    else_result,
                })
            }
        }
    }
}

impl<'a> From<&str> for ExprNode<'a> {
    fn from(expr: &str) -> Self {
        ExprNode::SqlExpr(expr.to_owned())
    }
}

impl<'a> From<i64> for ExprNode<'a> {
    fn from(n: i64) -> Self {
        ExprNode::Expr(Cow::Owned(Expr::Literal(AstLiteral::Number(
            BigDecimal::from(n),
        ))))
    }
}

impl<'a> From<bool> for ExprNode<'a> {
    fn from(b: bool) -> Self {
        ExprNode::Expr(Cow::Owned(Expr::Literal(AstLiteral::Boolean(b))))
    }
}

impl<'a> From<QueryNode<'a>> for ExprNode<'a> {
    fn from(node: QueryNode<'a>) -> Self {
        ExprNode::Subquery(Box::new(node))
    }
}

impl<'a> From<Expr> for ExprNode<'a> {
    fn from(expr: Expr) -> Self {
        ExprNode::Expr(Cow::Owned(expr))
    }
}

pub fn expr<'a>(value: &str) -> ExprNode<'a> {
    ExprNode::from(value)
}

pub fn col<'a>(value: &str) -> ExprNode<'a> {
    let idents = value.split('.').collect::<Vec<_>>();

    match idents.as_slice() {
        [alias, ident] => ExprNode::CompoundIdentifier {
            alias: alias.to_string(),
            ident: ident.to_string(),
        },
        _ => ExprNode::Identifier(value.to_owned()),
    }
}

pub fn num<'a>(value: i64) -> ExprNode<'a> {
    ExprNode::from(value)
}

pub fn text<'a>(value: &str) -> ExprNode<'a> {
    ExprNode::Expr(Cow::Owned(Expr::Literal(AstLiteral::QuotedString(
        value.to_owned(),
    ))))
}

pub fn date<'a>(date: &str) -> ExprNode<'a> {
    ExprNode::Expr(Cow::Owned(Expr::TypedString {
        data_type: DataType::Date,
        value: date.to_owned(),
    }))
}

pub fn timestamp<'a>(timestamp: &str) -> ExprNode<'a> {
    ExprNode::Expr(Cow::Owned(Expr::TypedString {
        data_type: DataType::Timestamp,
        value: timestamp.to_owned(),
    }))
}

pub fn time<'a>(time: &str) -> ExprNode<'a> {
    ExprNode::Expr(Cow::Owned(Expr::TypedString {
        data_type: DataType::Time,
        value: time.to_owned(),
    }))
}

pub fn subquery<'a, T: Into<QueryNode<'a>>>(query_node: T) -> ExprNode<'a> {
    ExprNode::Subquery(Box::new(query_node.into()))
}

#[cfg(test)]
mod tests {
    use {
        super::ExprNode,
        crate::{
            ast::Expr,
            ast_builder::{
                col, date, expr, num, subquery, table, test_expr, text, time, timestamp, QueryNode,
            },
        },
    };

    #[test]
    fn into_expr_node() {
        let actual: ExprNode = "id IS NOT NULL".into();
        let expected = "id IS NOT NULL";
        test_expr(actual, expected);

        let actual: ExprNode = 1024.into();
        let expected = "1024";
        test_expr(actual, expected);

        let actual: ExprNode = true.into();
        let expected = "True";
        test_expr(actual, expected);

        let actual: ExprNode = QueryNode::from(table("Foo").select().project("id")).into();
        let expected = "(SELECT id FROM Foo)";
        test_expr(actual, expected);

        let actual: ExprNode = Expr::Identifier("id".to_owned()).into();
        let expected = "id";
        test_expr(actual, expected);
    }

    #[test]
    fn syntactic_sugar() {
        let actual = expr("col1 > 10");
        let expected = "col1 > 10";
        test_expr(actual, expected);

        let actual = col("id");
        let expected = "id";
        test_expr(actual, expected);

        let actual = col("Foo.id");
        let expected = "Foo.id";
        test_expr(actual, expected);

        let actual = num(2048);
        let expected = "2048";
        test_expr(actual, expected);

        let actual = text("hello world");
        let expected = "'hello world'";
        test_expr(actual, expected);

        let actual = date("2022-10-11");
        let expected = "DATE '2022-10-11'";
        test_expr(actual, expected);

        let actual = timestamp("2022-10-11 13:34:49");
        let expected = "TIMESTAMP '2022-10-11 13:34:49'";
        test_expr(actual, expected);

        let actual = time("15:00:07");
        let expected = "TIME '15:00:07'";
        test_expr(actual, expected);

        let actual = subquery(table("Foo").select().filter("id IS NOT NULL"));
        let expected = "(SELECT * FROM Foo WHERE id IS NOT NULL)";
        test_expr(actual, expected);
    }
}
