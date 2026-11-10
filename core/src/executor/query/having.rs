use {
    super::aggregation::{self, AggregatedRows},
    crate::{
        executor::{
            context::{AggregateContext, RowContext, StatementContext},
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
    stmt_context: StatementContext<'a>,
    filter_context: Option<&Rc<RowContext<'a>>>,
    plan: &'a HavingPlan,
) -> Result<AggregatedRows<'a>>
where
    T: GStore,
{
    let HavingPlan { input, expr } = plan;
    let AggregatedRows { sources, rows } =
        aggregation::execute(storage, stmt_context, filter_context, input)?;
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

        if check_expr(
            storage,
            stmt_context,
            context.as_ref(),
            aggregated.as_ref(),
            expr,
        )? {
            filtered.push(AggregateContext { aggregated, next });
        }
    }

    Ok(AggregatedRows {
        sources,
        rows: filtered,
    })
}
