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
    },
    async_recursion::async_recursion,
    futures::stream::{self, Stream, StreamExt, TryStreamExt},
    std::{convert::identity, rc::Rc},
};

pub use error::AggregateError;

pub struct Aggregator<'a, T: GStore> {
    storage: &'a T,
    fields: &'a [SelectItem],
    group_by: &'a [Expr],
    having: Option<&'a Expr>,
    filter_context: Option<Rc<RowContext<'a>>>,
}

#[derive(futures_enum::Stream)]
enum S<T1, T2> {
    NonAggregate(T1),
    Aggregate(T2),
}

impl<'a, T: GStore> Aggregator<'a, T> {
    pub fn new(
        storage: &'a T,
        fields: &'a [SelectItem],
        group_by: &'a [Expr],
        having: Option<&'a Expr>,
        filter_context: Option<Rc<RowContext<'a>>>,
    ) -> Self {
        Self {
            storage,
            fields,
            group_by,
            having,
            filter_context,
        }
    }

    pub async fn apply(
        &self,
        rows: impl Stream<Item = Result<Rc<RowContext<'a>>>>,
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
                        Some(filter_context) => Rc::new(RowContext::concat(
                            Rc::clone(&project_context),
                            Rc::clone(filter_context),
                        )),
                        None => Rc::clone(&project_context),
                    };
                    let filter_context = Some(filter_context);

                    let evaluated: Vec<Evaluated<'_>> = stream::iter(self.group_by.iter())
                        .then(|expr| {
                            let filter_clone = filter_context.as_ref().map(Rc::clone);
                            async move { evaluate(self.storage, filter_clone, None, expr).await }
                        })
                        .try_collect::<Vec<_>>()
                        .await?;

                    let group = evaluated
                        .iter()
                        .map(Key::try_from)
                        .collect::<Result<Vec<Key>>>()?;

                    let state = state.apply(index, group, Rc::clone(&project_context));
                    let state = stream::iter(self.fields)
                        .map(Ok)
                        .try_fold(state, |state, field| {
                            let filter_clone = filter_context.as_ref().map(Rc::clone);

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

    pub async fn group_by_having(
        &self,
        state: State<'a, T>,
    ) -> Result<impl Stream<Item = Result<AggregateContext<'a>>>> {
        let storage = self.storage;
        let filter_context = self.filter_context.as_ref().map(Rc::clone);
        let having = self.having;
        let rows = state
            .export()
            .await?
            .into_iter()
            .filter_map(|(aggregated, next)| next.map(|next| (aggregated, next)));
        let rows = stream::iter(rows)
            .filter_map(move |(aggregated, next)| {
                let filter_context = filter_context.as_ref().map(Rc::clone);
                let aggregated = aggregated.map(Rc::new);

                async move {
                    match having {
                        None => Some(Ok((aggregated.as_ref().map(Rc::clone), next))),
                        Some(having) => {
                            let filter_context = match filter_context {
                                Some(filter_context) => {
                                    Rc::new(RowContext::concat(Rc::clone(&next), filter_context))
                                }
                                None => Rc::clone(&next),
                            };
                            let filter_context = Some(filter_context);
                            let aggregated = aggregated.as_ref().map(Rc::clone);

                            check_expr(
                                storage,
                                filter_context,
                                aggregated.as_ref().map(Rc::clone),
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
                    .map(Rc::try_unwrap)
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

#[async_recursion(?Send)]
async fn aggregate<'a, T>(
    state: State<'a, T>,
    filter_context: Option<Rc<RowContext<'a>>>,
    expr: &'a Expr,
) -> Result<State<'a, T>>
where
    T: GStore,
{
    let aggr = |state, expr| aggregate(state, filter_context.as_ref().map(Rc::clone), expr);

    match expr {
        Expr::Between {
            expr, low, high, ..
        } => {
            stream::iter([expr, low, high])
                .map(Ok)
                .try_fold(state, |state, expr| async move { aggr(state, expr).await })
                .await
        }
        Expr::BinaryOp { left, right, .. } => {
            stream::iter([left, right])
                .map(Ok)
                .try_fold(state, |state, expr| async move { aggr(state, expr).await })
                .await
        }
        Expr::UnaryOp { expr, .. } => aggr(state, expr).await,
        Expr::Nested(expr) => aggr(state, expr).await,
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            let operand = std::iter::once(operand.as_ref())
                .filter_map(|operand| operand.map(|operand| &**operand));
            let when_then = when_then
                .iter()
                .flat_map(|(when, then)| std::iter::once(when).chain(std::iter::once(then)));
            let else_result = std::iter::once(else_result.as_ref())
                .filter_map(|else_result| else_result.map(|else_result| &**else_result));

            stream::iter(operand.chain(when_then).chain(else_result).map(Ok))
                .try_fold(state, aggr)
                .await
        }
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
