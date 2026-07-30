use {
    super::{LabeledRows, SelectIter, select_node, select_order_by_node},
    crate::{
        executor::context::RowContext,
        plan::{DistinctInputPlan, DistinctPlan},
        result::Result,
        store::GStore,
    },
    std::{collections::HashSet, rc::Rc},
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a DistinctPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let LabeledRows { labels, rows } = match &plan.input {
        DistinctInputPlan::Select(select) => select_node::execute(storage, select, filter_context),
        DistinctInputPlan::SelectOrderBy(order_by) => {
            select_order_by_node::execute(storage, order_by, filter_context)
        }
    }?;
    let rows = rows.collect::<Result<Vec<_>>>()?;
    let mut seen = HashSet::new();
    let rows = rows
        .into_iter()
        .filter(move |row| seen.insert(row.values.clone()))
        .map(Ok);

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows) as SelectIter<'a>,
    })
}
