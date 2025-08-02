mod state;

use {
    self::state::State,
    super::{
        context::{AggregateContext, RowContext},
        evaluate::{Evaluated, evaluate},
        filter::check_expr,
    },
    crate::{
        ast::{Expr, SelectItem},
        data::Key,
        result::Result,
        store::GStore,
    },
    futures::{
        future::BoxFuture,
        stream::{self, Stream, StreamExt, TryStreamExt},
    },
    std::sync::Arc,
};

#[derive(futures_enum::Stream)]
enum S<T1, T2> {
    NonAggregate(T1),
    Aggregate(T2),
}

fn check_aggregate<'a>(fields: &'a [SelectItem], group_by: &'a [Expr]) -> bool {
    if !group_by.is_empty() {
        return true;
    }

    fields.iter().any(|field| match field {
        SelectItem::Expr { expr, .. } => check(expr),
        _ => false,
    })
}

pub async fn apply<'a, T: GStore, U: Stream<Item = Result<Arc<RowContext<'a>>>> + 'a>(
    storage: &'a T,
    fields: &'a [SelectItem],
    group_by: &'a [Expr],
    having: Option<&'a Expr>,
    filter_context: Option<Arc<RowContext<'a>>>,
    rows: U,
) -> Result<impl Stream<Item = Result<AggregateContext<'a>>> + use<'a, T, U>> {
    if !check_aggregate(fields, group_by) {
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
        .try_fold(State::new(storage), |state, (index, project_context)| {
            let filter_context = filter_context.clone();

            async move {
                let filter_context = match filter_context {
                    Some(filter_context) => Arc::new(RowContext::concat(
                        Arc::clone(&project_context),
                        filter_context,
                    )),
                    None => Arc::clone(&project_context),
                };
                let filter_context = Some(filter_context);

                let evaluated: Vec<Evaluated<'_>> = stream::iter(group_by.iter())
                    .then(|expr| {
                        let filter_clone = filter_context.as_ref().map(Arc::clone);
                        async move { evaluate(storage, filter_clone, None, expr).await }
                    })
                    .try_collect::<Vec<_>>()
                    .await?;

                let group = evaluated
                    .iter()
                    .map(Key::try_from)
                    .collect::<Result<Vec<Key>>>()?;

                let state = state.apply(index, group, Arc::clone(&project_context));
                let state = stream::iter(fields)
                    .map(Ok)
                    .try_fold(state, |state, field| {
                        let filter_clone = filter_context.as_ref().map(Arc::clone);

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
            }
        })
        .await?;

    group_by_having(storage, filter_context, having, state)
        .await
        .map(S::Aggregate)
}

async fn group_by_having<'a, T: GStore>(
    storage: &'a T,
    filter_context: Option<Arc<RowContext<'a>>>,
    having: Option<&'a Expr>,
    state: State<'a, T>,
) -> Result<impl Stream<Item = Result<AggregateContext<'a>>>> {
    let rows = state
        .export()
        .await?
        .into_iter()
        .filter_map(|(aggregated, next)| next.map(|next| (aggregated, next)));
    let rows = stream::iter(rows)
        .filter_map(move |(aggregated, next)| {
            let filter_context = filter_context.as_ref().map(Arc::clone);

            async move {
                match having {
                    None => Some(Ok((aggregated, next))),
                    Some(having) => {
                        let filter_context = match filter_context {
                            Some(filter_context) => {
                                Arc::new(RowContext::concat(Arc::clone(&next), filter_context))
                            }
                            None => Arc::clone(&next),
                        };
                        let filter_context = Some(filter_context);
                        let aggr_rc = aggregated.clone().map(Arc::new);

                        check_expr(storage, filter_context, aggr_rc, having)
                            .await
                            .map(|pass| pass.then_some((aggregated, next)))
                            .transpose()
                    }
                }
            }
        })
        .map(|res| res.map(|(aggregated, next)| AggregateContext { aggregated, next }));

    Ok(rows)
}

fn aggregate<'a, T>(
    state: State<'a, T>,
    filter_context: Option<Arc<RowContext<'a>>>,
    expr: &'a Expr,
) -> BoxFuture<'a, Result<State<'a, T>>>
where
    T: GStore + 'a,
{
    Box::pin(async move {
        match expr {
            Expr::Between {
                expr, low, high, ..
            } => {
                let state = aggregate(state, filter_context.clone(), expr).await?;
                let state = aggregate(state, filter_context.clone(), low).await?;
                aggregate(state, filter_context, high).await
            }
            Expr::BinaryOp { left, right, .. } => {
                let state = aggregate(state, filter_context.clone(), left).await?;
                aggregate(state, filter_context, right).await
            }
            Expr::UnaryOp { expr, .. } => aggregate(state, filter_context, expr).await,
            Expr::Nested(expr) => aggregate(state, filter_context, expr).await,
            Expr::Case {
                operand,
                when_then,
                else_result,
            } => {
                let mut state = match operand.as_deref() {
                    Some(op) => aggregate(state, filter_context.clone(), op).await?,
                    None => state,
                };

                for (when, then) in when_then {
                    state = aggregate(state, filter_context.clone(), when).await?;
                    state = aggregate(state, filter_context.clone(), then).await?;
                }

                if let Some(else_expr) = else_result.as_deref() {
                    state = aggregate(state, filter_context.clone(), else_expr).await?;
                }

                Ok(state)
            }
            Expr::Aggregate(aggr_expr) => {
                state.accumulate(filter_context, aggr_expr.as_ref()).await
            }
            _ => Ok(state),
        }
    })
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
                    .any(|(when, then)| check(when) || check(then))
                || else_result
                    .as_ref()
                    .map(|expr| check(expr))
                    .unwrap_or(false)
        }
        Expr::Aggregate(_) => true,
        _ => false,
    }
}
