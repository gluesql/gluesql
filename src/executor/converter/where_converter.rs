use {
    super::{
        super::evaluate::{evaluate, Evaluated},
        ConvertError,
    },
    crate::{Condition, Link, Result, Value},
    sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as Literal},
    std::convert::TryFrom,
};

macro_rules! value {
    ($value:expr) => {
        Value::try_from(&$value)?
    };
}

macro_rules! condition_result {
    ($value:expr) => {
        Ok(Link::Condition($value))
    };
}

/// Converts the SQL AST directly to a [crate::data::conditions::Link] type.
pub fn convert_where_query(where_expression: &Expr) -> Result<Link> {
    match where_expression {
        Expr::Value(_) | Expr::Wildcard => {
            // A where without any value is weird.
            Err(ConvertError::Unsupported.into())
        }
        Expr::IsNull(expr) => match *expr.clone() {
            Expr::Value(Literal::Null) => condition_result!(Condition::True),
            Expr::Identifier(column_name) => condition_result!(Condition::IsNull {
                column_name: column_name.value,
            }),
            _ => Err(ConvertError::Unimplemented(where_expression.to_string()).into()),
        },
        Expr::IsNotNull(expr) => match *expr.clone() {
            Expr::Value(value) => {
                condition_result!(Condition::from_bool(!matches!(value, Literal::Null)))
            }
            Expr::Identifier(column_name) => condition_result!(Condition::IsNotNull {
                column_name: column_name.value,
            }),
            _ => Err(ConvertError::Unimplemented(where_expression.to_string()).into()),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => match *expr.clone() {
            Expr::Value(value) => {
                let mut error = None;
                let result = list
                    .iter()
                    .find(|expr| match expr {
                        Expr::Value(inner_value) => value == *inner_value,
                        _ => {
                            error = Some(Err(ConvertError::Unimplemented(
                                where_expression.to_string(),
                            )
                            .into()));
                            true
                        }
                    })
                    .is_some();
                if let Some(error) = error {
                    error
                } else {
                    condition_result!(Condition::from_bool(result != *negated))
                }
            }
            Expr::Identifier(column_name) => {
                let mut error: Option<crate::result::Error> = None;
                let list_elem = list
                    .iter() // Doesn't stop at first error!
                    .map(|expr| match expr {
                        Expr::Value(value) => match Value::try_from(value) {
                            Ok(value) => value,
                            Err(err) => {
                                error = Some(err);
                                Value::Null
                            }
                        },
                        _ => {
                            error = Some(
                                ConvertError::Unimplemented(where_expression.to_string()).into(),
                            );
                            Value::Null
                        }
                    })
                    .collect();
                if let Some(error) = error {
                    Err(error)
                } else {
                    condition_result!(Condition::InList {
                        column_name: column_name.value,
                        list_elem
                    })
                }
            }
            _ => Err(ConvertError::Unimplemented(where_expression.to_string()).into()),
        },
        Expr::UnaryOp { op, expr } => match (op.clone(), *expr.clone()) {
            (UnaryOperator::Not, Expr::Value(expr)) => {
                condition_result!(Condition::from_bool(value!(expr) == Value::Bool(true)))
            }
            (UnaryOperator::Not, expr) => match convert_where_query(&expr) {
                Ok(Link::Condition(Condition::True)) => Ok(Link::Condition(Condition::False)),
                Ok(Link::Condition(Condition::False)) => Ok(Link::Condition(Condition::True)),
                Err(error) => Err(error),
                _ => Err(ConvertError::Unimplemented(where_expression.to_string()).into()),
            },
            _ => Err(ConvertError::Unimplemented(where_expression.to_string()).into()),
        },
        Expr::BinaryOp { left, op, right } => {
            match (*left.clone(), op.clone(), *right.clone()) {
                /*(Expr::Identifier(_), _, _) => {
                    unimplemented!("Comparing two rows is not implemented yet.")
                }
                (Expr::Subquery(_), _, _) => unimplemented!("Subqueries are not supported yet."),
                (_, _, Expr::Identifier(_)) => {
                    unimplemented!("Comparing two rows is not implemented yet.")
                }
                (_, _, Expr::Subquery(_)) => panic!("Subqueries are not supported yet."),*/
                /*(Expr::Value(left), BinaryOperator::Eq, Expr::Value(right)) => {
                    condition_result!(Condition::from_bool(value!(left) == value!(right)))
                }*/
                (Expr::Value(left), BinaryOperator::NotEq, Expr::Value(right)) => {
                    condition_result!(Condition::from_bool(value!(left) != value!(right)))
                }
                (Expr::Value(left), BinaryOperator::Gt, Expr::Value(right)) => {
                    condition_result!(Condition::from_bool(value!(left) > value!(right)))
                }
                (Expr::Value(left), BinaryOperator::GtEq, Expr::Value(right)) => {
                    condition_result!(Condition::from_bool(value!(left) >= value!(right)))
                }
                (Expr::Value(left), BinaryOperator::Lt, Expr::Value(right)) => {
                    condition_result!(Condition::from_bool(value!(left) < value!(right)))
                }
                (Expr::Value(left), BinaryOperator::LtEq, Expr::Value(right)) => {
                    condition_result!(Condition::from_bool(value!(left) <= value!(right)))
                }
                _ => Err(ConvertError::Unimplemented(where_expression.to_string()).into()),
            }
            /*match *left.clone() {
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
                }
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
                                }
                                BinaryOperator::NotEq => {
                                    if value != value_2 {
                                        return Ok(Link::Condition(Condition::True));
                                    }
                                    return Ok(Link::Condition(Condition::False));
                                }
                                BinaryOperator::Plus => {}
                                _ => {
                                    eprintln!("Expression: {:?}", where_expression);
                                    unimplemented!("Unsupported expression.")
                                }
                            }
                        }
                        Expr::Subquery(_) => {
                            unimplemented!("Subqueries are not implemented yet")
                        }
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
            };*/
        }
        Expr::Exists(query) => {
            return Ok(Link::Condition(Condition::ExistsSubquery {
                query: Box::from(*query.clone()),
            }))
        }

        _ => Err(ConvertError::Unimplemented(where_expression.to_string()).into()),
    }
}
