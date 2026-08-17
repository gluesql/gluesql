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
            context::{AggregateContext, RowContext},
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

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        name = "gluesql.query.aggregate",
        target = "gluesql",
        level = "debug",
        skip_all,
        fields(buffered_groups = tracing::field::Empty)
    )
)]
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
    let SelectedRows { sources, rows } = match input {
        AggregationInputPlan::Source(source) => source::execute(storage, source)?
            .rows(None)?
            .into_selected(None),
        AggregationInputPlan::InnerJoin(join) => inner::execute(storage, join, filter_context)?,
        AggregationInputPlan::LeftOuterJoin(join) => {
            left_outer::execute(storage, join, filter_context)?
        }
        AggregationInputPlan::Filter(filter) => filter::execute(storage, filter, filter_context)?,
    };
    let mut state = State::new(storage, aggregate_slots.len(), group_by.is_empty());

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
            .map(|expr| evaluate(storage, Some(&row_filter_context), None, expr)?.try_into())
            .collect::<Result<Vec<Value>>>()?;
        let group_index = state.apply(group, Rc::clone(&context));

        for (slot, aggregate) in aggregate_slots.iter().enumerate() {
            state.accumulate(group_index, &row_filter_context, slot, aggregate)?;
        }
    }

    let rows = state.export(aggregate_slots)?;

    #[cfg(feature = "tracing")]
    tracing::Span::current().record("buffered_groups", rows.len());

    Ok(AggregatedRows { sources, rows })
}
