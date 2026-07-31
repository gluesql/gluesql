mod state;

use {
    self::state::State,
    super::select_node,
    crate::{
        data::Value,
        executor::{
            context::{AggregateContext, RowContext},
            evaluate::evaluate,
        },
        plan::AggregationPlan,
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) type AggregatedRows<'a> = Vec<AggregateContext<'a>>;

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a AggregationPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<AggregatedRows<'a>>
where
    T: GStore,
{
    let AggregationPlan {
        input,
        group_by,
        aggregate_slots,
    } = plan;
    let rows = select_node::execute(storage, input, filter_context)?;
    let mut state = State::new(storage, aggregate_slots.len(), group_by.is_empty());

    for context in rows {
        let context = context?;
        let row_filter_context = match filter_context {
            Some(filter_context) => Some(Rc::new(RowContext::concat(
                Rc::clone(&context),
                Rc::clone(filter_context),
            ))),
            None => Some(Rc::clone(&context)),
        };
        let group = group_by
            .iter()
            .map(|expr| evaluate(storage, row_filter_context.as_ref(), None, expr)?.try_into())
            .collect::<Result<Vec<Value>>>()?;
        let group_index = state.apply(group, Rc::clone(&context));

        for (slot, aggregate) in aggregate_slots.iter().enumerate() {
            state.accumulate(group_index, row_filter_context.as_ref(), slot, aggregate)?;
        }
    }

    state.export(aggregate_slots)
}
