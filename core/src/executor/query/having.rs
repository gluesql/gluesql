use {
    super::aggregation::{self, AggregatedRows},
    crate::{
        executor::{
            context::{AggregateContext, RowContext},
            filter::check_expr,
        },
        plan::HavingPlan,
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a HavingPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<AggregatedRows<'a>>
where
    T: GStore,
{
    let HavingPlan { input, expr } = plan;
    let AggregatedRows { sources, rows } = aggregation::execute(storage, input, filter_context)?;
    let mut filtered = Vec::new();

    for aggregate_context in rows {
        let AggregateContext { aggregated, next } = aggregate_context;
        let context = match (&next, filter_context) {
            (Some(next), Some(filter_context)) => Some(Rc::new(RowContext::concat(
                Rc::clone(next),
                Rc::clone(filter_context),
            ))),
            (Some(next), None) => Some(Rc::clone(next)),
            (None, Some(filter_context)) => Some(Rc::clone(filter_context)),
            (None, None) => None,
        };

        if check_expr(storage, context.as_ref(), aggregated.as_ref(), expr)? {
            filtered.push(AggregateContext { aggregated, next });
        }
    }

    Ok(AggregatedRows {
        sources,
        rows: filtered,
    })
}
