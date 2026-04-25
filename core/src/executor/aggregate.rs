mod state;

use {
    self::state::State,
    super::{
        context::{AggregateContext, RowContext},
        evaluate::evaluate,
        filter::check_expr,
    },
    crate::{
        ast::{Aggregate, Expr},
        data::Value,
        result::Result,
        store::GStore,
    },
    futures::stream::{self, Stream, StreamExt, TryStreamExt},
    std::sync::Arc,
};

#[derive(futures_enum::Stream)]
enum S<T1, T2> {
    NonAggregate(T1),
    Aggregate(T2),
}

pub async fn apply<'a, T: GStore, U: Stream<Item = Result<Arc<RowContext<'a>>>> + 'a>(
    storage: &'a T,
    aggregate_slots: Option<&'a [Aggregate]>,
    group_by: &'a [Expr],
    having: Option<&'a Expr>,
    filter_context: Option<Arc<RowContext<'a>>>,
    rows: U,
) -> Result<impl Stream<Item = Result<AggregateContext<'a>>> + use<'a, T, U>> {
    let aggregate_slots = aggregate_slots.unwrap_or(&[]);
    let needs_aggregate = !group_by.is_empty() || !aggregate_slots.is_empty();

    if !needs_aggregate {
        let rows = rows.map_ok(|project_context| AggregateContext {
            aggregated: None,
            next: Some(project_context),
        });
        return Ok(S::NonAggregate(rows));
    }

    let state = rows
        .into_stream()
        .try_fold(
            State::new(storage, aggregate_slots.len(), group_by.is_empty()),
            |mut state, project_context| {
                let filter_context = filter_context.clone();

                async move {
                    let row_filter_context = match filter_context {
                        Some(filter_context) => Some(Arc::new(RowContext::concat(
                            Arc::clone(&project_context),
                            filter_context,
                        ))),
                        None => Some(Arc::clone(&project_context)),
                    };

                    let group = if group_by.is_empty() {
                        Vec::new()
                    } else {
                        stream::iter(group_by.iter())
                            .then(|expr| {
                                let row_filter_context =
                                    row_filter_context.as_ref().map(Arc::clone);

                                async move {
                                    evaluate(storage, row_filter_context, None, expr)
                                        .await?
                                        .try_into()
                                }
                            })
                            .try_collect::<Vec<Value>>()
                            .await?
                    };

                    let group_index = state.apply(group, Arc::clone(&project_context));
                    for (slot, aggregate) in aggregate_slots.iter().enumerate() {
                        let row_filter_context = row_filter_context.as_ref().map(Arc::clone);
                        state
                            .accumulate(group_index, row_filter_context, slot, aggregate)
                            .await?;
                    }

                    Ok(state)
                }
            },
        )
        .await?;

    Ok(S::Aggregate(group_by_having(
        storage,
        aggregate_slots,
        filter_context,
        having,
        state,
    )?))
}

fn group_by_having<'a, T: GStore>(
    storage: &'a T,
    aggregate_slots: &'a [Aggregate],
    filter_context: Option<Arc<RowContext<'a>>>,
    having: Option<&'a Expr>,
    state: State<'a, T>,
) -> Result<impl Stream<Item = Result<AggregateContext<'a>>>> {
    let rows = state.export(aggregate_slots)?.into_iter();
    let rows = stream::iter(rows).filter_map(move |aggregate_context| {
        let filter_context = filter_context.as_ref().map(Arc::clone);

        async move {
            let AggregateContext { aggregated, next } = aggregate_context;

            match having {
                None => Some(Ok(AggregateContext { aggregated, next })),
                Some(having) => {
                    let filter_context = match (&next, filter_context) {
                        (Some(next), Some(filter_context)) => Some(Arc::new(RowContext::concat(
                            Arc::clone(next),
                            filter_context,
                        ))),
                        (Some(next), None) => Some(Arc::clone(next)),
                        (None, Some(filter_context)) => Some(filter_context),
                        (None, None) => None,
                    };

                    check_expr(
                        storage,
                        filter_context,
                        aggregated.as_ref().map(Arc::clone),
                        having,
                    )
                    .await
                    .map(|pass| pass.then_some(AggregateContext { aggregated, next }))
                    .transpose()
                }
            }
        }
    });

    Ok(rows)
}
