use im_rc::HashMap;
use iter_enum::Iterator;
use serde::Serialize;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::iter::{empty, once};
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Expr, Function, SelectItem};

use super::context::{AggregateContext, BlendContext};
use crate::data::{get_name, Value};
use crate::result::Result;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum AggregateError {
    #[error("unsupported compound identifier: {0}")]
    UnsupportedCompoundIdentifier(String),

    #[error("unsupported aggregation: {0}")]
    UnsupportedAggregation(String),

    #[error("only identifier is allowed in aggregation")]
    OnlyIdentifierAllowed,

    #[error("unreachable")]
    Unreachable,
}

#[derive(Iterator)]
enum Aggregated<I1, I2, I3> {
    Applied(I1),
    Skipped(I2),
    Empty(I3),
}

pub struct Aggregate<'a> {
    fields: &'a [SelectItem],
}

impl<'a> Aggregate<'a> {
    pub fn new(fields: &'a [SelectItem]) -> Self {
        Self { fields }
    }

    pub fn apply(
        &self,
        rows: impl Iterator<Item = Result<Rc<BlendContext<'a>>>>,
    ) -> Result<impl Iterator<Item = Result<AggregateContext<'a>>>> {
        if !self.check_aggregate() {
            let rows = rows.map(|row| {
                row.map(|blend_context| AggregateContext {
                    aggregated: None,
                    next: blend_context,
                })
            });

            return Ok(Aggregated::Skipped(rows));
        }

        let (aggregated, next) = rows.enumerate().try_fold::<_, _, Result<_>>(
            (HashMap::<&Function, (usize, Value)>::new(), None),
            |(aggregated, _), (index, row)| {
                let context = row?;

                let aggregated = self
                    .fields
                    .iter()
                    .try_fold(aggregated, |aggregated, field| match field {
                        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                            aggregate(index, aggregated, &context, &expr)
                        }
                        _ => Ok(aggregated),
                    })?;

                Ok((aggregated, Some(context)))
            },
        )?;

        let next = match next {
            Some(next) => next,
            None => {
                return Ok(Aggregated::Empty(empty()));
            }
        };

        let aggregated: HashMap<&Function, Value> = aggregated
            .iter()
            .map(|(key, (_, value))| (*key, value.clone()))
            .collect();
        let aggregated = Some(aggregated);
        let rows = once(Ok(AggregateContext { aggregated, next }));

        Ok(Aggregated::Applied(rows))
    }

    fn check_aggregate(&self) -> bool {
        self.fields.iter().any(|field| match field {
            SelectItem::UnnamedExpr(expr) => check(expr),
            SelectItem::ExprWithAlias { expr, .. } => check(expr),
            _ => false,
        })
    }
}

fn aggregate<'a>(
    index: usize,
    aggregated: HashMap<&'a Function, (usize, Value)>,
    context: &BlendContext<'_>,
    expr: &'a Expr,
) -> Result<HashMap<&'a Function, (usize, Value)>> {
    let aggr = |aggregated, expr| aggregate(index, aggregated, context, expr);
    let get_value = |expr: &Expr| match expr {
        Expr::Identifier(ident) => context.get_value(&ident.value),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Err(AggregateError::UnsupportedCompoundIdentifier(expr.to_string()).into());
            }

            let table_alias = &idents[0].value;
            let column = &idents[1].value;

            context.get_alias_value(table_alias, column)
        }
        _ => Err(AggregateError::OnlyIdentifierAllowed.into()),
    };
    let get_first_value = |args: &[Expr]| {
        let expr = args.get(0).ok_or(AggregateError::Unreachable)?;

        get_value(expr)
    };

    match expr {
        Expr::Between {
            expr, low, high, ..
        } => [expr, low, high]
            .iter()
            .try_fold(aggregated, |aggregated, expr| aggr(aggregated, expr)),
        Expr::BinaryOp { left, right, .. } => [left, right]
            .iter()
            .try_fold(aggregated, |aggregated, expr| aggr(aggregated, expr)),
        Expr::UnaryOp { expr, .. } => aggr(aggregated, expr),
        Expr::Nested(expr) => aggr(aggregated, expr),
        Expr::Function(func) => {
            let Function { name, args, .. } = func;

            match get_name(name)?.to_uppercase().as_str() {
                "COUNT" => {
                    let expr = args.get(0).ok_or(AggregateError::Unreachable)?;

                    let value_to_incr = Value::I64(match expr {
                        Expr::Wildcard => 1,
                        _ => {
                            if get_value(expr)?.is_some() {
                                1
                            } else {
                                0
                            }
                        }
                    });

                    match aggregated.get(func) {
                        Some((current, value)) => {
                            if &index <= current {
                                return Ok(aggregated);
                            }

                            Ok(aggregated.update(func, (index, value_to_incr.add(value)?)))
                        }
                        None => Ok(aggregated.update(func, (index, value_to_incr))),
                    }
                }
                "SUM" => {
                    let value_to_sum = get_first_value(args)?;

                    match aggregated.get(func) {
                        Some((current, value)) => {
                            if &index <= current {
                                return Ok(aggregated);
                            }

                            Ok(aggregated.update(func, (index, value.add(value_to_sum)?)))
                        }
                        None => Ok(aggregated.update(func, (index, value_to_sum.clone()))),
                    }
                }
                "MAX" => {
                    let value_to_cmp = get_first_value(args)?;

                    match aggregated.get(func) {
                        Some((current, value)) => {
                            if &index <= current {
                                return Ok(aggregated);
                            }

                            match value.partial_cmp(value_to_cmp) {
                                Some(ordering) => match ordering {
                                    Ordering::Greater | Ordering::Equal => Ok(aggregated),
                                    Ordering::Less => {
                                        Ok(aggregated.update(func, (index, value_to_cmp.clone())))
                                    }
                                },
                                None => Ok(aggregated),
                            }
                        }
                        None => Ok(aggregated.update(func, (index, value_to_cmp.clone()))),
                    }
                }
                "MIN" => {
                    let value_to_cmp = get_first_value(args)?;

                    match aggregated.get(func) {
                        Some((current, value)) => {
                            if &index <= current {
                                return Ok(aggregated);
                            }

                            match value.partial_cmp(value_to_cmp) {
                                Some(ordering) => match ordering {
                                    Ordering::Less => Ok(aggregated),
                                    Ordering::Equal | Ordering::Greater => {
                                        Ok(aggregated.update(func, (index, value_to_cmp.clone())))
                                    }
                                },
                                None => Ok(aggregated),
                            }
                        }
                        None => Ok(aggregated.update(func, (index, value_to_cmp.clone()))),
                    }
                }
                name => Err(AggregateError::UnsupportedAggregation(name.to_string()).into()),
            }
        }
        _ => Ok(aggregated),
    }
}

fn check(expr: &Expr) -> bool {
    match expr {
        Expr::Between {
            expr, low, high, ..
        } => check(expr) || check(low) || check(high),
        Expr::BinaryOp { left, right, .. } => check(left) || check(right),
        Expr::UnaryOp { expr, .. } => check(expr),
        Expr::Nested(expr) => check(expr),
        Expr::Function(_) => true,
        _ => false,
    }
}
