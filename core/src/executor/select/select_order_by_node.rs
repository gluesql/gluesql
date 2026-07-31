mod error;

pub use error::SortError;
use {
    super::{
        LabeledRows, SelectIter,
        order_by::sort_by,
        project_node::{self, ProjectedRows},
    },
    crate::{
        ast::{Literal, UnaryOperator},
        data::{Row, Value},
        executor::{
            context::{AggregateValues, RowContext},
            evaluate::evaluate,
        },
        plan::{ExprPlan, OrderByExprPlan, SelectOrderByPlan},
        result::{Error, Result},
        store::GStore,
    },
    bigdecimal::ToPrimitive,
    std::{borrow::Cow, rc::Rc},
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a SelectOrderByPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let SelectOrderByPlan { input, exprs } = plan;
    let sort_context = filter_context.as_ref().map(Rc::clone);
    let ProjectedRows {
        labels,
        rows,
        table_alias,
    } = project_node::execute(storage, input, filter_context)?;
    let rows = sort(storage, sort_context.as_ref(), rows, table_alias, exprs)?;

    Ok(LabeledRows { labels, rows })
}

fn sort<'a, T>(
    storage: &'a T,
    context: Option<&Rc<RowContext<'a>>>,
    rows: impl Iterator<Item = Result<(Option<Rc<AggregateValues>>, Option<Rc<RowContext<'a>>>, Row)>>
    + 'a,
    table_alias: &'a str,
    order_by: &'a [OrderByExprPlan],
) -> Result<SelectIter<'a>>
where
    T: GStore,
{
    if order_by.is_empty() {
        return Ok(Box::new(rows.map(|row| row.map(|(.., row)| row))));
    }

    let rows = rows.collect::<Result<Vec<_>>>()?;
    let mut keyed_rows = Vec::with_capacity(rows.len());
    for (aggregated, next, row) in rows {
        enum SortType<'a> {
            Value(Value),
            Expr(&'a ExprPlan),
        }

        let order_by = order_by
            .iter()
            .map(|OrderByExprPlan { expr, asc }| -> Result<_> {
                let big_decimal = match expr {
                    ExprPlan::Literal(Literal::Number(n)) => Some(n),
                    ExprPlan::UnaryOp {
                        op: UnaryOperator::Plus,
                        expr,
                    } => match expr.as_ref() {
                        ExprPlan::Literal(Literal::Number(n)) => Some(n),
                        _ => None,
                    },
                    _ => None,
                };

                match big_decimal {
                    Some(n) => {
                        let index = n
                            .to_usize()
                            .ok_or_else(|| -> Error { SortError::Unreachable.into() })?;
                        let zero_based = index.checked_sub(1).ok_or_else(|| -> Error {
                            SortError::ColumnIndexOutOfRange(index).into()
                        })?;
                        let value = row.values.get(zero_based).ok_or_else(|| -> Error {
                            SortError::ColumnIndexOutOfRange(index).into()
                        })?;

                        Ok((SortType::Value(value.clone()), *asc))
                    }
                    _ => Ok((SortType::Expr(expr), *asc)),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let filter_context = match (&next, &context) {
            (Some(next), Some(context)) => Some(Rc::new(RowContext::concat(
                Rc::clone(next),
                Rc::clone(context),
            ))),
            (Some(next), None) => Some(Rc::clone(next)),
            (None, Some(context)) => Some(Rc::clone(context)),
            (None, None) => None,
        };

        let context = RowContext::new(table_alias, Cow::Borrowed(&row), None);
        let label_context = Rc::new(context);
        let filter_context = match filter_context {
            Some(filter_context) => Some(Rc::new(RowContext::concat(
                filter_context,
                Rc::clone(&label_context),
            ))),
            None => Some(Rc::clone(&label_context)),
        };

        let keys = order_by
            .into_iter()
            .map(|(sort_type, asc)| {
                match sort_type {
                    SortType::Value(value) => Ok(value),
                    SortType::Expr(expr) => {
                        evaluate(storage, filter_context.as_ref(), aggregated.as_ref(), expr)?
                            .try_into()
                    }
                }?
                .try_into()
                .map(|key| (key, asc))
            })
            .collect::<Result<Vec<_>>>()?;

        keyed_rows.push((keys, row));
    }

    keyed_rows.sort_by(|(keys_a, ..), (keys_b, ..)| sort_by(keys_a, keys_b));

    let rows = keyed_rows.into_iter().map(|(.., row)| row).map(Ok);

    Ok(Box::new(rows))
}
