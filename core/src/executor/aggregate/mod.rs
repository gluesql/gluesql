mod error;
mod state;

use {
    self::state::State,
    super::{
        context::{AggregateContext, BlendContext, FilterContext},
        evaluate::{evaluate, Evaluated},
        filter::check_expr,
    },
    crate::{
        ast::{Expr, SelectItem},
        data::Key,
        result::{Error, Result},
        store::GStore,
    },
    async_recursion::async_recursion,
    futures::stream::{self, StreamExt, TryStream, TryStreamExt},
    std::{convert::identity, pin::Pin, rc::Rc},
};

pub use error::AggregateError;

pub struct Aggregator<'a> {
    storage: &'a dyn GStore,
    fields: &'a [SelectItem],
    group_by: &'a [Expr],
    having: Option<&'a Expr>,
    filter_context: Option<Rc<FilterContext<'a>>>,
}

type Applied<'a> = dyn TryStream<Ok = AggregateContext<'a>, Error = Error, Item = Result<AggregateContext<'a>>>
    + 'a;

impl<'a> Aggregator<'a> {
    pub fn new(
        storage: &'a dyn GStore,
        fields: &'a [SelectItem],
        group_by: &'a [Expr],
        having: Option<&'a Expr>,
        filter_context: Option<Rc<FilterContext<'a>>>,
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
        rows: impl TryStream<Ok = Rc<BlendContext<'a>>, Error = Error> + 'a,
    ) -> Result<Pin<Box<Applied<'a>>>> {
        if !self.check_aggregate() {
            let rows = rows.map_ok(|blend_context| AggregateContext {
                aggregated: None,
                next: blend_context,
            });
            return Ok(Box::pin(rows));
        }

        let state = rows
            .into_stream()
            .enumerate()
            .map(|(i, row)| row.map(|row| (i, row)))
            .try_fold(
                State::new(self.storage),
                |state, (index, blend_context)| async move {
                    let filter_context = FilterContext::concat(
                        self.filter_context.as_ref().map(Rc::clone),
                        Some(&blend_context).map(Rc::clone),
                    );
                    let filter_context = Some(filter_context).map(Rc::new);

                    let evaluated: Vec<Evaluated<'_>> = stream::iter(self.group_by.iter())
                        .then(|expr| {
                            let filter = filter_context.as_ref().map(Rc::clone);
                            async move { evaluate(self.storage, filter, None, expr).await }
                        })
                        .try_collect::<Vec<_>>()
                        .await?;

                    let group = evaluated
                        .iter()
                        .map(Key::try_from)
                        .collect::<Result<Vec<Key>>>()?;

                    let state = state.apply(index, group, Rc::clone(&blend_context));
                    let state = stream::iter(self.fields)
                        .fold(Ok(state), |state, field| async move {
                            match field {
                                SelectItem::Expr { expr, .. } => {
                                    aggregate(state?, self.filter_context.clone(), expr).await
                                }
                                _ => state,
                            }
                        })
                        .await?;

                    Ok(state)
                },
            )
            .await?;

        self.group_by_having(state)
    }

    pub fn group_by_having(&self, state: State<'a>) -> Result<Pin<Box<Applied<'a>>>> {
        let storage = self.storage;
        let filter_context = self.filter_context.as_ref().map(Rc::clone);
        let having = self.having;
        let rows = state
            .export()?
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
                            let filter_context =
                                FilterContext::concat(filter_context, Some(Rc::clone(&next)));
                            let filter_context = Some(filter_context).map(Rc::new);
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
            .and_then(|(aggregated, next): (Option<_>, _)| async move {
                aggregated
                    .map(Rc::try_unwrap)
                    .transpose()
                    .map_err(|_| AggregateError::UnreachableRcUnwrapFailure.into())
                    .map(|aggregated| AggregateContext { aggregated, next })
            });

        Ok(Box::pin(rows))
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
async fn aggregate<'a>(
    state: State<'a>,
    filter_context: Option<Rc<FilterContext<'a>>>,
    expr: &'a Expr,
) -> Result<State<'a>> {
    let aggr = |state, expr| aggregate(state, filter_context.as_ref().map(Rc::clone), expr);
    match expr {
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
        Expr::Aggregate(_) => true,
        _ => false,
    }
}
