mod error;
mod state;

use {
    self::state::State,
    super::{
        context::{AggregateContext, RowContext},
        evaluate::{evaluate, Evaluated},
        filter::check_expr,
    },
    crate::{
        ast::{Expr, SelectItem},
        data::Key,
        result::Result,
        store::GStore,
        Grc,
    },
    async_recursion::async_recursion,
    futures::stream::{self, Stream, StreamExt, TryStreamExt},
    std::convert::identity,
};

pub use error::AggregateError;

pub struct Aggregator<'a, T> {
    storage: &'a T,
    fields: &'a [SelectItem],
    group_by: &'a [Expr],
    having: Option<&'a Expr>,
    filter_context: Option<Grc<RowContext<'a>>>,
}

#[derive(futures_enum::Stream)]
enum S<T1, T2> {
    NonAggregate(T1),
    Aggregate(T2),
}

impl<
        'a,
        #[cfg(feature = "send")] T: GStore + Send + Sync,
        #[cfg(not(feature = "send"))] T: GStore,
    > Aggregator<'a, T>
{
    pub fn new(
        storage: &'a T,
        fields: &'a [SelectItem],
        group_by: &'a [Expr],
        having: Option<&'a Expr>,
        filter_context: Option<Grc<RowContext<'a>>>,
    ) -> Self {
        Self {
            storage,
            fields,
            group_by,
            having,
            filter_context,
        }
    }

    // these two same fns can be replaced with a impl type alias for the return type once its stabilized (https://rust-lang.github.io/impl-trait-initiative/explainer/tait.html)
    #[cfg(not(feature = "send"))]
    pub async fn apply(
        &self,
        rows: impl Stream<Item = Result<Grc<RowContext<'a>>>>,
    ) -> Result<impl Stream<Item = Result<AggregateContext<'a>>>> {
        if !self.check_aggregate() {
            let rows = rows.map_ok(|project_context| AggregateContext {
                aggregated: None,
                next: project_context,
            });
            return Ok(S::NonAggregate(rows));
        }

        let state = rows
            .into_stream()
            .enumerate()
            .map(|(i, row)| row.map(|row| (i, row)))
            .try_fold(
                State::new(self.storage),
                |state, (index, project_context)| async move {
                    let filter_context = match &self.filter_context {
                        Some(filter_context) => Grc::new(RowContext::concat(
                            Grc::clone(&project_context),
                            Grc::clone(filter_context),
                        )),
                        None => Grc::clone(&project_context),
                    };
                    let filter_context = Some(filter_context);

                    let evaluated: Vec<Evaluated<'_>> = stream::iter(self.group_by.iter())
                        .then(|expr| {
                            let filter_clone = filter_context.as_ref().map(Grc::clone);
                            async move { evaluate(self.storage, filter_clone, None, expr).await }
                        })
                        .try_collect::<Vec<_>>()
                        .await?;

                    let group = evaluated
                        .iter()
                        .map(Key::try_from)
                        .collect::<Result<Vec<Key>>>()?;

                    let state = state.apply(index, group, Grc::clone(&project_context));
                    let state = stream::iter(self.fields)
                        .map(Ok)
                        .try_fold(state, |state, field| {
                            let filter_clone = filter_context.as_ref().map(Grc::clone);

                            async move {
                                match field {
                                    SelectItem::Expr { expr, .. } => {
                                        aggregate(state, filter_clone, expr).await
                                    }
                                    _ => Ok(state),
                                }
                            }
                        })
                        .await?;

                    Ok(state)
                },
            )
            .await?;

        self.group_by_having(state).await.map(S::Aggregate)
    }

    // these two same fns can be replaced with a impl type alias for the return type once its stabilized (https://rust-lang.github.io/impl-trait-initiative/explainer/tait.html)
    #[cfg(feature = "send")]
    pub async fn apply(
        &self,
        rows: impl Stream<Item = Result<Grc<RowContext<'a>>>> + Send,
    ) -> Result<impl Stream<Item = Result<AggregateContext<'a>>> + Send> {
        if !self.check_aggregate() {
            let rows = rows.map_ok(|project_context| AggregateContext {
                aggregated: None,
                next: project_context,
            });
            return Ok(S::NonAggregate(rows));
        }

        let state = rows
            .into_stream()
            .enumerate()
            .map(|(i, row)| row.map(|row| (i, row)))
            .try_fold(
                State::new(self.storage),
                |state, (index, project_context)| async move {
                    let filter_context = match &self.filter_context {
                        Some(filter_context) => Grc::new(RowContext::concat(
                            Grc::clone(&project_context),
                            Grc::clone(filter_context),
                        )),
                        None => Grc::clone(&project_context),
                    };
                    let filter_context = Some(filter_context);

                    let evaluated: Vec<Evaluated<'_>> = stream::iter(self.group_by.iter())
                        .then(|expr| {
                            let filter_clone = filter_context.as_ref().map(Grc::clone);
                            async move { evaluate(self.storage, filter_clone, None, expr).await }
                        })
                        .try_collect::<Vec<_>>()
                        .await?;

                    let group = evaluated
                        .iter()
                        .map(Key::try_from)
                        .collect::<Result<Vec<Key>>>()?;

                    let state = state.apply(index, group, Grc::clone(&project_context));
                    let state = stream::iter(self.fields)
                        .map(Ok)
                        .try_fold(state, |state, field| {
                            let filter_clone = filter_context.as_ref().map(Grc::clone);

                            async move {
                                match field {
                                    SelectItem::Expr { expr, .. } => {
                                        aggregate(state, filter_clone, expr).await
                                    }
                                    _ => Ok(state),
                                }
                            }
                        })
                        .await?;

                    Ok(state)
                },
            )
            .await?;

        self.group_by_having(state).await.map(S::Aggregate)
    }

    // these two same fns can be replaced with a impl type alias for the return type once its stabilized (https://rust-lang.github.io/impl-trait-initiative/explainer/tait.html)
    #[cfg(not(feature = "send"))]
    pub async fn group_by_having(
        &self,
        state: State<'a, T>,
    ) -> Result<impl Stream<Item = Result<AggregateContext<'a>>>> {
        let storage = self.storage;
        let filter_context = self.filter_context.as_ref().map(Grc::clone);
        let having = self.having;
        let rows = state
            .export()
            .await?
            .into_iter()
            .filter_map(|(aggregated, next)| next.map(|next| (aggregated, next)));

        let rows = stream::iter(rows)
            .filter_map(move |(aggregated, next)| {
                let filter_context = filter_context.as_ref().map(Grc::clone);
                let aggregated = aggregated.map(Grc::new);

                async move {
                    match having {
                        None => Some(Ok((aggregated.as_ref().map(Grc::clone), next))),
                        Some(having) => {
                            let filter_context = match filter_context {
                                Some(filter_context) => {
                                    Grc::new(RowContext::concat(Grc::clone(&next), filter_context))
                                }
                                None => Grc::clone(&next),
                            };
                            let filter_context = Some(filter_context);
                            let aggregated = aggregated.as_ref().map(Grc::clone);

                            check_expr(
                                storage,
                                filter_context,
                                aggregated.as_ref().map(Grc::clone),
                                having,
                            )
                            .await
                            .map(|pass| pass.then_some((aggregated, next)))
                            .transpose()
                        }
                    }
                }
            })
            .and_then(|(aggregated, next)| async move {
                aggregated
                    .map(Grc::try_unwrap)
                    .transpose()
                    .map_err(|_| AggregateError::UnreachableRcUnwrapFailure.into())
                    .map(|aggregated| AggregateContext { aggregated, next })
            });

        Ok(rows)
    }

    // these two same fns can be replaced with a impl type alias for the return type once its stabilized (https://rust-lang.github.io/impl-trait-initiative/explainer/tait.html)
    #[cfg(feature = "send")]
    pub async fn group_by_having(
        &self,
        state: State<'a, T>,
    ) -> Result<impl Stream<Item = Result<AggregateContext<'a>>> + Send> {
        let storage = self.storage;
        let filter_context = self.filter_context.as_ref().map(Grc::clone);
        let having = self.having;
        let rows = state
            .export()
            .await?
            .into_iter()
            .filter_map(|(aggregated, next)| next.map(|next| (aggregated, next)));

        let rows = stream::iter(rows)
            .filter_map(move |(aggregated, next)| {
                let filter_context = filter_context.as_ref().map(Grc::clone);
                let aggregated = aggregated.map(Grc::new);

                async move {
                    match having {
                        None => Some(Ok((aggregated.as_ref().map(Grc::clone), next))),
                        Some(having) => {
                            let filter_context = match filter_context {
                                Some(filter_context) => {
                                    Grc::new(RowContext::concat(Grc::clone(&next), filter_context))
                                }
                                None => Grc::clone(&next),
                            };
                            let filter_context = Some(filter_context);
                            let aggregated = aggregated.as_ref().map(Grc::clone);

                            check_expr(
                                storage,
                                filter_context,
                                aggregated.as_ref().map(Grc::clone),
                                having,
                            )
                            .await
                            .map(|pass| pass.then_some((aggregated, next)))
                            .transpose()
                        }
                    }
                }
            })
            .and_then(|(aggregated, next)| async move {
                aggregated
                    .map(Grc::try_unwrap)
                    .transpose()
                    .map_err(|_| AggregateError::UnreachableRcUnwrapFailure.into())
                    .map(|aggregated| AggregateContext { aggregated, next })
            });

        Ok(rows)
    }

    fn check_aggregate(&self) -> bool {
        if !self.group_by.is_empty() {
            return true;
        }

        self.fields
            .iter()
            .map(|field| match field {
                SelectItem::Expr { expr, .. } => check(expr),
                _ => false,
            })
            .any(identity)
    }
}

#[cfg_attr(not(feature = "send"), async_recursion(?Send))]
#[cfg_attr(feature = "send", async_recursion)]
async fn aggregate<
    'a,
    #[cfg(feature = "send")] T: GStore + Send + Sync,
    #[cfg(not(feature = "send"))] T: GStore,
>(
    state: State<'a, T>,
    filter_context: Option<Grc<RowContext<'a>>>,
    expr: &'a Expr,
) -> Result<State<'a, T>> {
    let aggr =
        |state: State<'a, T>, expr| aggregate(state, filter_context.as_ref().map(Grc::clone), expr);

    match expr {
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            // not using iters and chaining due to cryptic lifetimes issues
            let mut exprs = vec![];

            if let Some(e) = operand {
                exprs.push(&**e)
            }

            let mut when_then_iter = when_then.into_iter();
            while let Some((when, then)) = when_then_iter.next() {
                exprs.push(when);
                exprs.push(then);
            }

            if let Some(e) = else_result {
                exprs.push(&**e)
            }

            stream::iter(exprs)
                .fold(
                    Ok(state),
                    |state, expr| async move { aggr(state?, expr).await },
                )
                .await
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            stream::iter([expr, low, high])
                .fold(
                    Ok(state),
                    |state, expr| async move { aggr(state?, expr).await },
                )
                .await
        }
        Expr::BinaryOp { left, right, .. } => {
            stream::iter([left, right])
                .fold(
                    Ok(state),
                    |state, expr| async move { aggr(state?, expr).await },
                )
                .await
        }
        Expr::UnaryOp { expr, .. } => aggr(state, expr).await,
        Expr::Nested(expr) => aggr(state, expr).await,
        Expr::Aggregate(aggr) => state.accumulate(filter_context, aggr.as_ref()).await,
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
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            operand.as_ref().map(|expr| check(expr)).unwrap_or(false)
                || when_then
                    .iter()
                    .map(|(when, then)| check(when) || check(then))
                    .any(identity)
                || else_result
                    .as_ref()
                    .map(|expr| check(expr))
                    .unwrap_or(false)
        }
        Expr::Aggregate(_) => true,
        _ => false,
    }
}
