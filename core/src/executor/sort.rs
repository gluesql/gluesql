use {
    super::{context::RowContext, evaluate::evaluate},
    crate::{
        ast::{Aggregate, AstLiteral, Expr, OrderByExpr, UnaryOperator},
        data::{Key, Row, Value},
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

                    let keys = order_by
                        .map(stream::iter)?
                        .then(|(sort_type, asc)| {
                            let context = Some(Rc::clone(&filter_context));
                            let aggregated = aggregated.as_ref().map(Rc::clone);

                            async move {
                                match sort_type {
                                    SortType::Value(value) => value,
                                    SortType::Expr(expr) => {
                                        evaluate(self.storage, context, aggregated, expr)
                                            .await?
                                            .try_into()?
                                    }
                                }
                                .try_into()
                                .map(|key| (key, asc))
                            }
                        })
                        .try_collect::<Vec<_>>()
                        .await?;

                    drop(label_context);
                    drop(filter_context);

                    Ok((keys, row))
                }
            })
            .try_collect::<Vec<(Vec<(Key, Option<bool>)>, Row)>>()
            .await
            .map(Vector::from)?
            .sort_by(|(keys_a, ..), (keys_b, ..)| sort_by(keys_a, keys_b))
            .into_iter()
            .map(|(.., row)| Ok(row));

        Ok(Rows::OrderBy(stream::iter(rows)))
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
            (Ordering::Equal, _) => continue,
            (ord, true) => return ord,
            (ord, false) => return ord.reverse(),
        }
    }

    Ordering::Equal
}
