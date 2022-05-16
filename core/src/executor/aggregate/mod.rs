mod error;
mod hash;
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
        result::{Error, Result},
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStream, TryStreamExt},
    std::{pin::Pin, rc::Rc},
};

pub use {error::AggregateError, hash::GroupKey};

pub struct Aggregator<'a, T> {
    storage: &'a dyn GStore<T>,
    fields: &'a [SelectItem],
    group_by: &'a [Expr],
    having: Option<&'a Expr>,
    filter_context: Option<Rc<FilterContext<'a>>>,
}

type Applied<'a> = dyn TryStream<Ok = AggregateContext<'a>, Error = Error, Item = Result<AggregateContext<'a>>>
    + 'a;

impl<'a, T> Aggregator<'a, T> {
    pub fn new(
        storage: &'a dyn GStore<T>,
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
        if !self.check_aggregate()? {
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
            .try_fold(State::new(), |state, (index, blend_context)| async move {
                let evaluated: Vec<Evaluated<'_>> = stream::iter(self.group_by.iter())
                    .then(|expr| {
                        let filter_context = FilterContext::concat(
                            self.filter_context.as_ref().map(Rc::clone),
                            Some(&blend_context).map(Rc::clone),
                        );
                        let filter_context = Some(filter_context).map(Rc::new);

                        async move { evaluate(self.storage, filter_context, None, expr).await }
                    })
                    .try_collect::<Vec<_>>()
                    .await?;
                let group = evaluated
                    .iter()
                    .map(GroupKey::try_from)
                    .collect::<Result<Vec<GroupKey>>>()?;

                let state = state.apply(index, group, Rc::clone(&blend_context));
                let state = self
                    .fields
                    .iter()
                    .try_fold(state, |state, field| match field {
                        SelectItem::Expr { expr, .. } => aggregate(state, &blend_context, expr),
                        _ => Ok(state),
                    })?;

                Ok(state)
            })
            .await?;

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
                            .map(|pass| pass.then(|| (aggregated, next)))
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

    fn check_aggregate(&self) -> Result<bool> {
        if !self.group_by.is_empty() {
            return Ok(true);
        }

        self.fields
            .iter()
            .map(|field| match field {
                SelectItem::Expr { expr, .. } => check(expr),
                _ => Ok(false),
            })
            .collect::<Result<Vec<bool>>>()
            .map(|checked| checked.into_iter().any(|c| c))
    }
}

fn aggregate<'a>(
    state: State<'a>,
    context: &BlendContext<'_>,
    expr: &'a Expr,
) -> Result<State<'a>> {
    let aggr = |state, expr| aggregate(state, context, expr);

    match expr {
        Expr::Between {
            expr, low, high, ..
        } => [expr, low, high]
            .iter()
            .try_fold(state, |state, expr| aggr(state, expr)),
        Expr::BinaryOp { left, right, .. } => [left, right]
            .iter()
            .try_fold(state, |state, expr| aggr(state, expr)),
        Expr::UnaryOp { expr, .. } => aggr(state, expr),
        Expr::Nested(expr) => aggr(state, expr),
        Expr::Aggregate(aggr) => state.accumulate(context, aggr.as_ref()),
        _ => Ok(state),
    }
}

fn check(expr: &Expr) -> Result<bool> {
    let checked = match expr {
        Expr::Between {
            expr, low, high, ..
        } => check(expr)? || check(low)? || check(high)?,
        Expr::BinaryOp { left, right, .. } => check(left)? || check(right)?,
        Expr::UnaryOp { expr, .. } => check(expr)?,
        Expr::Nested(expr) => check(expr)?,
        Expr::Aggregate(_) => true,
        _ => false,
    };

    Ok(checked)
}
