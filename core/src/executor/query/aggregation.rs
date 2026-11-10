mod state;

use {
    self::state::State,
    super::{
        SelectedRows, SelectedSources, filter,
        join::{inner, left_outer},
        source,
    },
    crate::{
        data::Value,
        executor::{
            context::{AggregateContext, RowContext, StatementContext},
            evaluate::evaluate,
        },
        plan::{AggregationInputPlan, AggregationPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) struct AggregatedRows<'a> {
    pub(super) sources: SelectedSources<'a>,
    pub(super) rows: Vec<AggregateContext<'a>>,
}

pub(super) fn execute<'a, T>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    filter_context: Option<&Rc<RowContext<'a>>>,
    plan: &'a AggregationPlan,
) -> Result<AggregatedRows<'a>>
where
    T: GStore,
{
    let AggregationPlan {
        input,
        group_by,
        aggregate_slots,
    } = plan;
    let SelectedRows { sources, rows } = match input {
        AggregationInputPlan::Source(source) => source::execute(storage, stmt_context, source)?
            .rows(filter_context.map(Rc::clone))?
            .into_selected(None),
        AggregationInputPlan::InnerJoin(join) => {
            inner::execute(storage, stmt_context, filter_context, join)?
        }
        AggregationInputPlan::LeftOuterJoin(join) => {
            left_outer::execute(storage, stmt_context, filter_context, join)?
        }
        AggregationInputPlan::Filter(filter) => {
            filter::execute(storage, stmt_context, filter_context, filter)?
        }
    };
    let mut state = State::new(
        storage,
        stmt_context,
        aggregate_slots.len(),
        group_by.is_empty(),
    );

    for context in rows {
        let context = context?;
        let row_filter_context = match filter_context {
            Some(filter_context) => Rc::new(RowContext::concat(
                Rc::clone(&context),
                Rc::clone(filter_context),
            )),
            None => Rc::clone(&context),
        };
        let group = group_by
            .iter()
            .map(|expr| {
                evaluate(storage, stmt_context, Some(&row_filter_context), None, expr)?.try_into()
            })
            .collect::<Result<Vec<Value>>>()?;
        let group_index = state.apply(group, Rc::clone(&context));

        for (slot, aggregate) in aggregate_slots.iter().enumerate() {
            state.accumulate(group_index, &row_filter_context, slot, aggregate)?;
        }
    }

    let rows = state.export(aggregate_slots)?;

    Ok(AggregatedRows { sources, rows })
}
