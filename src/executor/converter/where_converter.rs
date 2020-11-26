use sqlparser::ast::Expr;
use crate::{Link, Condition, Value, Error, ValueError};
use crate::Result;
use sqlparser::ast::Expr::Case;
use std::convert::TryFrom;
use std::fs::read;
use sqlparser::ast::BinaryOperator;

/// Converts the SQL AST directly to a [crate::data::conditions::Link] type.
pub fn convert_where_query(where_expression: &Expr) -> Result<Link> {
    match where_expression {
        Expr::Value(_) | Expr::Wildcard => {
            // A where without any value is weird.
            unreachable!()
        },
        Expr::BinaryOp { left, op, right } => {
            match *left.clone() {
                Expr::Identifier(value) => {
                    let column = value.value.clone();
                    let value;
                    match *right.clone() {
                        Expr::Identifier(_) => unimplemented!("Comparing two rows is not implemented yet."),
                        Expr::Value(val) => value = val,
                        // The subquery will need some more work
                        Expr::Subquery(_) => {
                            unimplemented!("Subqueries are not supported yet.")
                        },
                        _ => {
                            eprintln!("Expression: {:?}", where_expression);
                            unimplemented!("Unsupported expression.")
                        }
                    }
                    match *op.clone() {

                    }
                },
                Expr::Value(value) => {
                    let column;
                    let value = Value::try_from(&value)?;
                    match *right.clone() {
                        Expr::Identifier(col) => column = col,
                        Expr::Value(value_2) => {
                            let value_2 = Value::try_from(&value_2)?;
                            // Do operations
                            match op.clone() {
                                BinaryOperator::Eq => {
                                    if value == value_2 {
                                        return Ok(Link::Condition(Condition::True));
                                    }
                                    return Ok(Link::Condition(Condition::False));
                                },
                                BinaryOperator::NotEq => {
                                    if value != value_2 {
                                        return Ok(Link::Condition(Condition::True));
                                    }
                                    return Ok(Link::Condition(Condition::False));
                                }
                                BinaryOperator::Plus => {

                                },
                                _ => {
                                    eprintln!("Expression: {:?}", where_expression);
                                    unimplemented!("Unsupported expression.")
                                }
                            }
                        },
                        Expr::Subquery(_) => {
                            unimplemented!("Subqueries are not implemented yet")
                        },
                        _ => {
                            eprintln!("Expression: {:?}", where_expression);
                            unimplemented!("Unsupported expression.")
                        }
                    }
                }
                _ => {
                    eprintln!("Expression: {:?}", where_expression);
                    unimplemented!("Unsupported expression.")
                }
            };
        }
        Expr::Exists(query) => {
            return Ok(Link::Condition(Condition::ExistsSubquery { query: Box::from(*query.clone()) }))
        }

        _ => {
            eprintln!("Expression: {:?}", where_expression);
            unimplemented!("Unsupported expression.");
        }
    }
    eprintln!("Expression: {:?}", where_expression);
    unimplemented!("Unsupported expression.");
}