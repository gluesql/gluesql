mod error;
mod hash;
mod state;

use iter_enum::Iterator;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::rc::Rc;

use sqlparser::ast::{Expr, Function, SelectItem};

use super::context::{AggregateContext, BlendContext, FilterContext, UnionContext};
use super::evaluate::{evaluate_union, Evaluated};
use crate::data::{get_name, Value};
use crate::result::Result;
use crate::store::Store;

pub use error::AggregateError;
pub use hash::GroupKey;
use state::State;

#[derive(Iterator)]
enum Aggregated<I1, I2> {
    Applied(I1),
    Skipped(I2),
}

pub struct Aggregate<'a, T: 'static + Debug> {
    storage: &'a dyn Store<T>,
    fields: &'a [SelectItem],
    group_by: &'a [Expr],
    filter_context: Option<&'a FilterContext<'a>>,
}

impl<'a, T: 'static + Debug> Aggregate<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        fields: &'a [SelectItem],
        group_by: &'a [Expr],
        filter_context: Option<&'a FilterContext<'a>>,
    ) -> Self {
        Self {
            storage,
            fields,
            group_by,
            filter_context,
        }
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

        let state =
            rows.enumerate()
                .try_fold::<_, _, Result<_>>(State::new(), |state, (index, row)| {
                    let context = row?;
                    let evaluated: Vec<Evaluated<'_>> = self
                        .group_by
                        .iter()
                        .map(|expr| {
                            let union_context =
                                UnionContext::new(self.filter_context, Some(&context));

                            evaluate_union(self.storage, union_context, None, expr)
                        })
                        .collect::<Result<_>>()?;
                    let group = evaluated
                        .iter()
                        .map(GroupKey::try_from)
                        .collect::<Result<Vec<GroupKey>>>()?;

                    let state = state.apply(group, Rc::clone(&context), index);
                    let state = self
                        .fields
                        .iter()
                        .try_fold(state, |state, field| match field {
                            SelectItem::UnnamedExpr(expr)
                            | SelectItem::ExprWithAlias { expr, .. } => {
                                aggregate(state, &context, &expr)
                            }
                            _ => Ok(state),
                        })?;

                    Ok(state)
                })?;

        let rows = state
            .export()
            .into_iter()
            .filter_map(|(aggregated, next)| next.map(|next| (aggregated, next)))
            .map(|(aggregated, next)| Ok(AggregateContext { aggregated, next }));

        Ok(Aggregated::Applied(rows))
    }

    fn check_aggregate(&self) -> bool {
        if !self.group_by.is_empty() {
            return true;
        }

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
