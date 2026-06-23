use {
    super::{
        context::{AggregateValues, RowContext},
        evaluate::evaluate,
    },
    crate::{
        ast::{Literal, UnaryOperator},
        data::{Key, Row, Value},
        plan::{ExprPlan, OrderByExprPlan},
        result::{Error, Result},
        store::GStore,
    },
    bigdecimal::ToPrimitive,
    serde::Serialize,
    std::{borrow::Cow, cmp::Ordering, fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum SortError {
    #[error("ORDER BY COLUMN_INDEX must be within SELECT-list but: {0}")]
    ColumnIndexOutOfRange(usize),
    #[error("Unreachable ORDER BY Clause")]
    Unreachable,
}

pub struct Sort<'a, T: GStore> {
    storage: &'a T,
    context: Option<Rc<RowContext<'a>>>,
    order_by: &'a [OrderByExprPlan],
}

impl<'a, T: GStore> Sort<'a, T> {
    pub fn new(
        storage: &'a T,
        context: Option<Rc<RowContext<'a>>>,
        order_by: &'a [OrderByExprPlan],
    ) -> Self {
        Self {
            storage,
            context,
            order_by,
        }
    }

    pub fn apply(
        &self,
        rows: impl Iterator<
            Item = Result<(Option<Rc<AggregateValues>>, Option<Rc<RowContext<'a>>>, Row)>,
        > + 'a,
        table_alias: &'a str,
    ) -> Result<Box<dyn Iterator<Item = Result<Row>> + 'a>> {
        if self.order_by.is_empty() {
            return Ok(Box::new(rows.map(|row| row.map(|(.., row)| row))));
        }

        let rows = rows.collect::<Result<Vec<_>>>()?;
        let mut keyed_rows = Vec::with_capacity(rows.len());
        for (aggregated, next, row) in rows {
            enum SortType<'a> {
                Value(Value),
                Expr(&'a ExprPlan),
            }

            let order_by = self
                .order_by
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

            let filter_context = match (&next, &self.context) {
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
                        SortType::Expr(expr) => evaluate(
                            self.storage,
                            filter_context.as_ref(),
                            aggregated.as_ref(),
                            expr,
                        )?
                        .try_into(),
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
}

pub fn sort_by(keys_a: &[(Key, Option<bool>)], keys_b: &[(Key, Option<bool>)]) -> Ordering {
    let pairs = keys_a
        .iter()
        .map(|(a, _)| a)
        .zip(keys_b.iter())
        .map(|(a, (b, asc))| (a, b, asc.unwrap_or(true)));

    for (key_a, key_b, asc) in pairs {
        match (key_a.cmp(key_b), asc) {
            (Ordering::Equal, _) => {}
            (ord, true) => return ord,
            (ord, false) => return ord.reverse(),
        }
    }

    Ordering::Equal
}
