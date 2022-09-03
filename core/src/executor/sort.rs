use crate::ast::UnaryOperator;

use {
    super::{
        context::{AggregateContext, FilterContext},
        evaluate::evaluate,
    },
    crate::{
        ast::{AstLiteral, Expr, OrderByExpr},
        data::{Row, Value},
        executor::context::BlendContext,
        result::{Error, Result},
        store::GStore,
    },
    bigdecimal::ToPrimitive,
    futures::stream::{self, Stream, StreamExt, TryStreamExt},
    serde::Serialize,
    std::fmt::Debug,
    std::{cmp::Ordering, rc::Rc},
    thiserror::Error as ThisError,
    utils::Vector,
};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum SortError {
    #[error("ORDER BY COLUMN_INDEX must be within SELECT-list but: {0}")]
    ColumnIndexOutOfRange(usize),
    #[error("Unreachable ORDER BY Clause")]
    Unreachable,
}

pub struct Sort<'a> {
    storage: &'a dyn GStore,
    context: Option<Rc<FilterContext<'a>>>,
    order_by: &'a [OrderByExpr],
}

impl<'a> Sort<'a> {
    pub fn new(
        storage: &'a dyn GStore,
        context: Option<Rc<FilterContext<'a>>>,
        order_by: &'a [OrderByExpr],
    ) -> Self {
        Self {
            storage,
            context,
            order_by,
        }
    }

    pub async fn apply(
        &self,
        rows: impl Stream<Item = Result<(AggregateContext<'a>, Row)>> + 'a,
        labels: Rc<Vec<String>>,
        table_alias: &'a str,
    ) -> Result<impl Stream<Item = Result<Row>> + 'a> {
        #[derive(futures_enum::Stream)]
        enum Rows<I1, I2> {
            NonOrderBy(I1),
            OrderBy(I2),
        }
        if self.order_by.is_empty() {
            let rows = rows.map_ok(|(_, row)| row);

            return Ok(Rows::NonOrderBy(Box::pin(rows)));
        }
        let rows = rows
            .and_then(|(AggregateContext { aggregated, next }, row)| {
                enum SortType<'a> {
                    Value(Value),
                    Expr(&'a Expr),
                }

                let order_by = self
                    .order_by
                    .iter()
                    .map(|OrderByExpr { expr, asc }| -> Result<_> {
                        let big_decimal = match expr {
                            Expr::Literal(AstLiteral::Number(n)) => Some(n),
                            Expr::UnaryOp {
                                op: UnaryOperator::Plus,
                                expr,
                            } => match expr.as_ref() {
                                Expr::Literal(AstLiteral::Number(n)) => Some(n),
                                _ => None,
                            },
                            _ => None,
                        };
                        match big_decimal {
                            Some(n) => {
                                let index = n.to_usize().ok_or_else(|| {
                                    crate::result::Error::from(SortError::Unreachable)
                                })?;
                                let zero_based = index.checked_sub(1).ok_or_else(|| {
                                    crate::result::Error::from(SortError::ColumnIndexOutOfRange(
                                        index,
                                    ))
                                })?;
                                let value = row.get_value(zero_based).ok_or_else(|| {
                                    crate::result::Error::from(SortError::ColumnIndexOutOfRange(
                                        index,
                                    ))
                                })?;

                                Ok((SortType::Value(value.clone()), *asc))
                            }
                            None => Ok((SortType::Expr(expr), *asc)),
                        }
                    })
                    .collect::<Result<Vec<_>>>();
                // let table_alias = next.get_table_alias();
                let labels = Rc::from(labels.as_slice());
                let filter_context = Rc::new(FilterContext::concat(
                    self.context.as_ref().map(Rc::clone),
                    Some(Rc::clone(&next)),
                ));
                let aggregated = aggregated.map(Rc::new);
                async move {
                    let label_context = BlendContext::new(table_alias, labels, Some(row), None);
                    let label_context = Rc::from(label_context);
                    let filter_context = Rc::new(FilterContext::concat(
                        Some(filter_context),
                        Some(Rc::clone(&label_context)),
                    ));

                    let order_by = order_by?;

                    let values = stream::iter(order_by.into_iter())
                        .then(|(sort_type, asc)| {
                            let context = Some(Rc::clone(&filter_context));
                            let aggregated = aggregated.as_ref().map(Rc::clone);

                            async move {
                                let value: Value = match sort_type {
                                    SortType::Value(value) => value,
                                    SortType::Expr(expr) => {
                                        evaluate(self.storage, context, aggregated, expr)
                                            .await?
                                            .try_into()?
                                    }
                                };

                                Ok::<_, Error>((value, asc))
                            }
                        })
                        .try_collect::<Vec<_>>()
                        .await?;
                    drop(filter_context);
                    let row = Rc::try_unwrap(label_context).unwrap().row.unwrap();

                    Ok((values, row))
                }
            })
            .try_collect::<Vec<(Vec<(Value, Option<bool>)>, Row)>>()
            .await
            .map(Vector::from)?
            .sort_by(|(values_a, ..), (values_b, ..)| Self::sort_by(values_a, values_b))
            .into_iter()
            .map(|(.., row)| Ok(row));

        Ok(Rows::OrderBy(stream::iter(rows)))
    }

    pub fn sort_by(
        values_a: &[(Value, Option<bool>)],
        values_b: &[(Value, Option<bool>)],
    ) -> Ordering {
        let pairs = values_a
            .iter()
            .map(|(a, _)| a)
            .zip(values_b.iter())
            .map(|(a, (b, asc))| (a, b, asc.unwrap_or(true)));

        for (value_a, value_b, asc) in pairs {
            let apply_asc = |ord: Ordering| if asc { ord } else { ord.reverse() };

            match (value_a, value_b) {
                (Value::Null, Value::Null) => {}
                (Value::Null, _) => {
                    return apply_asc(Ordering::Greater);
                }
                (_, Value::Null) => {
                    return apply_asc(Ordering::Less);
                }
                _ => {}
            };

            match value_a.partial_cmp(value_b) {
                Some(ord) if ord != Ordering::Equal => {
                    return apply_asc(ord);
                }
                _ => {}
            }
        }

        Ordering::Equal
    }
}
