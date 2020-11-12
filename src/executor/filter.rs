use async_recursion::async_recursion;
use boolinator::Boolinator;
use im_rc::HashMap;
use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{BinaryOperator, Expr, Function, UnaryOperator};

use super::context::{BlendContext, FilterContext};
use super::evaluate::{evaluate, Evaluated};
use super::select::select;
use crate::data::Value;
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
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Function, Value>>>,
}

impl<'a, T: 'static + Debug> Filter<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        where_clause: Option<&'a Expr>,
        context: Option<Rc<FilterContext<'a>>>,
        aggregated: Option<Rc<HashMap<&'a Function, Value>>>,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
            aggregated,
        }
    }

    pub async fn check(&self, blend_context: Rc<BlendContext<'a>>) -> Result<bool> {
        match self.where_clause {
            Some(expr) => {
                let context = self.context.as_ref().map(Rc::clone);
                let context = FilterContext::concat(context, Some(blend_context));
                let context = Some(context).map(Rc::new);
                let aggregated = self.aggregated.as_ref().map(Rc::clone);

                check_expr(self.storage, context, aggregated, expr).await
            }
            None => Ok(true),
        }
    }
}

#[async_recursion(?Send)]
pub async fn check_expr<T: 'static + Debug>(
    storage: &dyn Store<T>,
    filter_context: Option<Rc<FilterContext<'async_recursion>>>,
    aggregated: Option<Rc<HashMap<&'async_recursion Function, Value>>>,
    expr: &Expr,
) -> Result<bool> {
    let evaluate = |expr: &'async_recursion Expr| {
        let filter_context = filter_context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate(storage, filter_context, aggregated, expr)
    };
    let check = |expr| {
        let filter_context = filter_context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        check_expr(storage, filter_context, aggregated, expr)
    };

    match expr {
        Expr::BinaryOp { op, left, right } => {
            let zip_evaluate = || Ok((evaluate(left)?, evaluate(right)?));
            let zip_check = || async move {
                let l = check(left).await?;
                let r = check(right).await?;

                Ok((l, r))
            };

            match op {
                BinaryOperator::Eq => zip_evaluate().map(|(l, r)| l == r),
                BinaryOperator::NotEq => zip_evaluate().map(|(l, r)| l != r),
                BinaryOperator::And => zip_check().await.map(|(l, r)| l && r),
                BinaryOperator::Or => zip_check().await.map(|(l, r)| l || r),
                BinaryOperator::Lt => zip_evaluate().map(|(l, r)| l < r),
                BinaryOperator::LtEq => zip_evaluate().map(|(l, r)| l <= r),
                BinaryOperator::Gt => zip_evaluate().map(|(l, r)| l > r),
                BinaryOperator::GtEq => zip_evaluate().map(|(l, r)| l >= r),
                _ => Err(FilterError::Unimplemented.into()),
            }
        }
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => check(&expr).await.map(|v| !v),
            _ => Err(FilterError::Unimplemented.into()),
        },
        Expr::Nested(expr) => check(&expr).await,
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
        Expr::Exists(query) => Ok(select(storage, query, filter_context)?.next().is_some()),
        Expr::IsNull(expr) => Ok(!evaluate(expr)?.is_some()),
        Expr::IsNotNull(expr) => Ok(evaluate(expr)?.is_some()),
        _ => Err(FilterError::Unimplemented.into()),
    }
}
