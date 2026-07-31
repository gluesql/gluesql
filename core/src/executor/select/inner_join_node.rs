use {
    super::{
        JoinCandidates, SelectedIter, SelectedRows, hash_join_node, join_condition_node,
        nested_loop_join_node,
    },
    crate::{
        executor::context::RowContext,
        plan::{InnerJoinInputPlan, InnerJoinPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    plan: &'a InnerJoinPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SelectedRows<'a>> {
    let JoinCandidates {
        sources, groups, ..
    } = match &plan.input {
        InnerJoinInputPlan::NestedLoop(join) => {
            nested_loop_join_node::execute(storage, join, filter_context)?
        }
        InnerJoinInputPlan::Hash(join) => hash_join_node::execute(storage, join, filter_context)?,
        InnerJoinInputPlan::Condition(condition) => {
            join_condition_node::execute(storage, condition, filter_context)?
        }
    };
    let rows = groups.flat_map(|group| match group {
        Ok(group) => group.rows,
        Err(error) => Box::new(std::iter::once(Err(error))) as SelectedIter<'a>,
    });

    Ok(SelectedRows {
        sources,
        rows: Box::new(rows),
    })
}
