mod state;

use {
    self::state::State,
    super::{
        context::{AggregateContext, RowContext},
        evaluate::evaluate,
        filter::check_expr,
    },
    crate::{
        data::Value,
        plan::{AggregatePlan, ExprPlan},
        result::Result,
        store::GStore,
    },
    std::sync::Arc,
};

pub type AggregateIter<'a> = Box<dyn Iterator<Item = Result<AggregateContext<'a>>> + Send + 'a>;

pub fn apply<'a, T: GStore>(
    storage: &'a T,
    aggregate_slots: Option<&'a [AggregatePlan]>,
    group_by: &'a [ExprPlan],
    having: Option<&'a ExprPlan>,
    filter_context: Option<&Arc<RowContext<'a>>>,
    rows: Box<dyn Iterator<Item = Result<Arc<RowContext<'a>>>> + Send + 'a>,
) -> Result<AggregateIter<'a>> {
    let aggregate_slots = aggregate_slots.unwrap_or(&[]);
    let needs_aggregate = !group_by.is_empty() || !aggregate_slots.is_empty();

    if !needs_aggregate {
        return Ok(Box::new(rows.map(|project_context| {
            let project_context = project_context?;

            Ok(AggregateContext {
                aggregated: None,
                next: Some(project_context),
            })
        })));
    }

    let mut state = State::new(storage, aggregate_slots.len(), group_by.is_empty());
    for project_context in rows {
        let project_context = project_context?;
        let row_filter_context = match filter_context {
            Some(filter_context) => Some(Arc::new(RowContext::concat(
                Arc::clone(&project_context),
                Arc::clone(filter_context),
            ))),
            None => Some(Arc::clone(&project_context)),
        };

        let group = group_by
            .iter()
            .map(|expr| evaluate(storage, row_filter_context.as_ref(), None, expr)?.try_into())
            .collect::<Result<Vec<Value>>>()?;

        let group_index = state.apply(group, Arc::clone(&project_context));
        for (slot, aggregate) in aggregate_slots.iter().enumerate() {
            state.accumulate(group_index, row_filter_context.as_ref(), slot, aggregate)?;
        }
    }

    group_by_having(
        storage,
        filter_context,
        having,
        state.export(aggregate_slots)?,
    )
    .map(|rows| Box::new(rows.into_iter().map(Ok)) as AggregateIter<'a>)
}

fn group_by_having<'a, T: GStore>(
    storage: &'a T,
    filter_context: Option<&Arc<RowContext<'a>>>,
    having: Option<&'a ExprPlan>,
    rows: Vec<AggregateContext<'a>>,
) -> Result<Vec<AggregateContext<'a>>> {
    let mut filtered = Vec::new();

    for aggregate_context in rows {
        let AggregateContext { aggregated, next } = aggregate_context;

        let pass = match having {
            None => true,
            Some(having) => {
                let filter_context = match (&next, filter_context) {
                    (Some(next), Some(filter_context)) => Some(Arc::new(RowContext::concat(
                        Arc::clone(next),
                        Arc::clone(filter_context),
                    ))),
                    (Some(next), None) => Some(Arc::clone(next)),
                    (None, Some(filter_context)) => Some(Arc::clone(filter_context)),
                    (None, None) => None,
                };

                check_expr(
                    storage,
                    filter_context.as_ref(),
                    aggregated.as_ref(),
                    having,
                )?
            }
        };

        if pass {
            filtered.push(AggregateContext { aggregated, next });
        }
    }

    Ok(filtered)
}
