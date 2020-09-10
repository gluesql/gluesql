mod error;
mod state;

use iter_enum::Iterator;
use std::iter::{empty, once};
use std::rc::Rc;

use sqlparser::ast::{Expr, Function, SelectItem};

use super::context::{AggregateContext, BlendContext};
// use super::evaluate::{evaluate, Evaluated};
use crate::data::{get_name, Value};
use crate::result::Result;

pub use error::AggregateError;
use state::State;

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

        let (state, next) = rows.enumerate().try_fold::<_, _, Result<_>>(
            (State::new(), None),
            |(state, _), (index, row)| {
                let context = row?;
                let state = state.apply(index);

                let state = self
                    .fields
                    .iter()
                    .try_fold(state, |state, field| match field {
                        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                            aggregate(state, &context, &expr)
                        }
                        _ => Ok(state),
                    })?;

                Ok((state, Some(context)))
            },
        )?;

        let next = match next {
            Some(next) => next,
            None => {
                return Ok(Aggregated::Empty(empty()));
            }
        };

        let aggregated = Some(state.export());
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
    state: State<'a>,
    context: &BlendContext<'_>,
    expr: &'a Expr,
) -> Result<State<'a>> {
    let aggr = |state, expr| aggregate(state, context, expr);
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
            .try_fold(state, |state, expr| aggr(state, expr)),
        Expr::BinaryOp { left, right, .. } => [left, right]
            .iter()
            .try_fold(state, |state, expr| aggr(state, expr)),
        Expr::UnaryOp { expr, .. } => aggr(state, expr),
        Expr::Nested(expr) => aggr(state, expr),
        Expr::Function(func) => {
            let Function { name, args, .. } = func;

            match get_name(name)?.to_uppercase().as_str() {
                "COUNT" => {
                    let expr = args.get(0).ok_or(AggregateError::Unreachable)?;
                    let value = Value::I64(match expr {
                        Expr::Wildcard => 1,
                        _ => {
                            if get_value(expr)?.is_some() {
                                1
                            } else {
                                0
                            }
                        }
                    });

                    state.add(func, &value)
                }
                "SUM" => state.add(func, get_first_value(args)?),
                "MAX" => Ok(state.set_max(func, get_first_value(args)?)),
                "MIN" => Ok(state.set_min(func, get_first_value(args)?)),
                name => Err(AggregateError::UnsupportedAggregation(name.to_string()).into()),
            }
        }
        _ => Ok(state),
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
