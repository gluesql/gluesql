use {
    super::{context::RowContext, evaluate::evaluate},
    crate::{
        ast::{Aggregate, AstLiteral, Expr, OrderByExpr, UnaryOperator},
        data::{Row, Value},
        result::{Error, Result},
        store::GStore,
    },
    bigdecimal::ToPrimitive,
    futures::stream::{self, Stream, StreamExt, TryStreamExt},
    im_rc::HashMap,
    serde::Serialize,
    std::{borrow::Cow, cmp::Ordering, fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
    utils::Vector,
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
    order_by: &'a [OrderByExpr],
}

impl<'a, T: GStore> Sort<'a, T> {
    pub fn new(
        storage: &'a T,
        context: Option<Rc<RowContext<'a>>>,
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
        rows: impl Stream<
                Item = Result<(
                    Option<Rc<HashMap<&'a Aggregate, Value>>>,
                    Rc<RowContext<'a>>,
                    Row,
                )>,
            > + 'a,
        table_alias: &'a str,
    ) -> Result<impl Stream<Item = Result<Row>> + 'a> {
        #[derive(futures_enum::Stream)]
        enum Rows<I1, I2> {
            NonOrderBy(I1),
            OrderBy(I2),
        }

        if self.order_by.is_empty() {
            let rows = rows.map_ok(|(.., row)| row);

            return Ok(Rows::NonOrderBy(Box::pin(rows)));
        }

        let rows = rows
            .and_then(|(aggregated, next, row)| {
                enum SortType<'a> {
                    Value(Value),
                    Expr(&'a Expr),
                }

                let order_by = self.order_by;
                let order_by = order_by
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

                        match (big_decimal, &row) {
                            (Some(n), Row::Vec { values, .. }) => {
                                let index = n
                                    .to_usize()
                                    .ok_or_else(|| -> Error { SortError::Unreachable.into() })?;
                                let zero_based = index.checked_sub(1).ok_or_else(|| -> Error {
                                    SortError::ColumnIndexOutOfRange(index).into()
                                })?;
                                let value = values.get(zero_based).ok_or_else(|| -> Error {
                                    SortError::ColumnIndexOutOfRange(index).into()
                                })?;

                                Ok((SortType::Value(value.clone()), *asc))
                            }
                            _ => Ok((SortType::Expr(expr), *asc)),
                        }
                    })
                    .collect::<Result<Vec<_>>>();

                let filter_context = match &self.context {
                    Some(context) => {
                        Rc::new(RowContext::concat(Rc::clone(&next), Rc::clone(context)))
                    }
                    None => Rc::clone(&next),
                };

                async move {
                    let context = RowContext::new(table_alias, Cow::Borrowed(&row), None);
                    let label_context = Rc::new(context);
                    let filter_context = Rc::new(RowContext::concat(
                        filter_context,
                        Rc::clone(&label_context),
                    ));

                    let order_by = order_by?;

                    let values = stream::iter(order_by.into_iter());
                    let values = values
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

                    drop(label_context);
                    drop(filter_context);

                    Ok((values, row))
                }
            })
            .try_collect::<Vec<(Vec<(Value, Option<bool>)>, Row)>>()
            .await
            .map(Vector::from)?
            .sort_by(|(values_a, ..), (values_b, ..)| sort_by(values_a, values_b))
            .into_iter()
            .map(|(.., row)| Ok(row));

        Ok(Rows::OrderBy(stream::iter(rows)))
    }
}

pub fn sort_by(values_a: &[(Value, Option<bool>)], values_b: &[(Value, Option<bool>)]) -> Ordering {
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
