use boolinator::Boolinator;
use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::{BinaryOperator, Expr, Ident, UnaryOperator};

use super::context::{BlendContext, FilterContext};
use super::evaluate::{evaluate, Evaluated};
use super::select::select;
use crate::data::Row;
use crate::result::Result;
use crate::store::Store;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum FilterError {
    #[error("unimplemented")]
    Unimplemented,
}

pub struct Filter<'a, T: 'static + Debug> {
    storage: &'a dyn Store<T>,
    where_clause: Option<&'a Expr>,
    context: Option<&'a FilterContext<'a>>,
}

impl<'a, T: 'static + Debug> Filter<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        where_clause: Option<&'a Expr>,
        context: Option<&'a FilterContext<'a>>,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
        }
    }

    pub fn check(&self, table_alias: &str, columns: &[Ident], row: &Row) -> Result<bool> {
        let context = FilterContext::new(table_alias, columns, row, self.context);

        match self.where_clause {
            Some(expr) => check_expr(self.storage, Some(context).as_ref(), expr),
            None => Ok(true),
        }
    }

    pub fn check_blended(&self, blend_context: &BlendContext<'_>) -> Result<bool> {
        match self.where_clause {
            Some(expr) => check_blended_expr(self.storage, self.context, blend_context, expr),
            None => Ok(true),
        }
    }
}

pub struct BlendedFilter<'a, T: 'static + Debug> {
    filter: &'a Filter<'a, T>,
    context: Option<&'a BlendContext<'a>>,
}

impl<'a, T: 'static + Debug> BlendedFilter<'a, T> {
    pub fn new(filter: &'a Filter<'a, T>, context: Option<&'a BlendContext<'a>>) -> Self {
        Self { filter, context }
    }

    pub fn check(&self, table_alias: &str, columns: &[Ident], row: &Row) -> Result<bool> {
        let BlendedFilter {
            filter:
                Filter {
                    storage,
                    where_clause,
                    context: next,
                },
            context: blend_context,
        } = self;

        let filter_context = FilterContext::new(table_alias, columns, row, *next);
        let filter_context = Some(&filter_context);

        where_clause.map_or(Ok(true), |expr| match blend_context {
            Some(blend_context) => {
                check_blended_expr(*storage, filter_context, blend_context, expr)
            }
            None => check_expr(*storage, filter_context, expr),
        })
    }
}

fn check_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: Option<&'a FilterContext<'a>>,
    expr: &'a Expr,
) -> Result<bool> {
    let evaluate = |expr| evaluate(storage, filter_context, None, expr);
    let check = |expr| check_expr(storage, filter_context, expr);
    match expr {
        Expr::BinaryOp { op, left, right } => {
            let zip_evaluate = || Ok((evaluate(left)?, evaluate(right)?));
            let zip_check = || Ok((check(left)?, check(right)?));

            match op {
                BinaryOperator::Eq => zip_evaluate().map(|(l, r)| l == r),
                BinaryOperator::NotEq => zip_evaluate().map(|(l, r)| l != r),
                BinaryOperator::And => zip_check().map(|(l, r)| l && r),
                BinaryOperator::Or => zip_check().map(|(l, r)| l || r),
                BinaryOperator::Lt => zip_evaluate().map(|(l, r)| l < r),
                BinaryOperator::LtEq => zip_evaluate().map(|(l, r)| l <= r),
                BinaryOperator::Gt => zip_evaluate().map(|(l, r)| l > r),
                BinaryOperator::GtEq => zip_evaluate().map(|(l, r)| l >= r),
                _ => Err(FilterError::Unimplemented.into()),
            }
        }
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => check(&expr).map(|v| !v),
            _ => Err(FilterError::Unimplemented.into()),
        },
        Expr::Nested(expr) => check(&expr),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let negated = *negated;
            let target = evaluate(expr)?;

            list.iter()
                .filter_map(|expr| {
                    evaluate(expr).map_or_else(
                        |error| Some(Err(error)),
                        |evaluated| (target == evaluated).as_some(Ok(!negated)),
                    )
                })
                .next()
                .unwrap_or(Ok(negated))
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let negated = *negated;
            let target = evaluate(expr)?;

            select(storage, &subquery, filter_context)?
                .map(|row| row?.take_first_value())
                .filter_map(|value| {
                    value.map_or_else(
                        |error| Some(Err(error)),
                        |value| (target == Evaluated::ValueRef(&value)).as_some(Ok(!negated)),
                    )
                })
                .next()
                .unwrap_or(Ok(negated))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let negated = *negated;
            let target = evaluate(expr)?;

            Ok(negated ^ (evaluate(low)? <= target && target <= evaluate(high)?))
        }
        Expr::IsNull(expr) => Ok(match evaluate(expr)? {
            Evaluated::ValueRef(v) => !v.is_some(),
            Evaluated::Value(v) => !v.is_some(),
            _ => false,
        }),
        Expr::IsNotNull(expr) => Ok(match evaluate(expr)? {
            Evaluated::ValueRef(v) => v.is_some(),
            Evaluated::Value(v) => v.is_some(),
            _ => false,
        }),
        _ => Err(FilterError::Unimplemented.into()),
    }
}

fn check_blended_expr<T: 'static + Debug>(
    storage: &dyn Store<T>,
    filter_context: Option<&FilterContext<'_>>,
    blend_context: &BlendContext<'_>,
    expr: &Expr,
) -> Result<bool> {
    let BlendContext {
        table_alias,
        columns,
        row,
        next,
        ..
    } = blend_context;

    let row_context = row
        .as_ref()
        .map(|row| FilterContext::new(table_alias, &columns, row, filter_context));
    let filter_context = row_context.as_ref().or(filter_context);

    match next {
        Some(blend_context) => check_blended_expr(storage, filter_context, blend_context, expr),
        None => check_expr(storage, filter_context, expr),
    }
}
