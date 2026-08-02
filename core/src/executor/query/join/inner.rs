use {
    super::super::{SelectedIter, SelectedRows},
    super::{JoinCandidates, condition, hash, nested_loop},
    crate::{
        executor::context::RowContext,
        plan::{InnerJoinInputPlan, InnerJoinPlan},
        result::Result,
        store::GStore,
    },
    std::{iter, rc::Rc},
};

pub(crate) fn execute<'a, T: GStore>(
    storage: &'a T,
    plan: &'a InnerJoinPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SelectedRows<'a>> {
    let JoinCandidates {
        sources, groups, ..
    } = match &plan.input {
        InnerJoinInputPlan::NestedLoop(join) => {
            nested_loop::execute(storage, join, filter_context)?
        }
        InnerJoinInputPlan::Hash(join) => hash::execute(storage, join, filter_context)?,
        InnerJoinInputPlan::Condition(condition) => {
            condition::execute(storage, condition, filter_context)?
        }
    };
    let rows = groups.flat_map(|group| match group {
        Ok(group) => group.rows,
        Err(error) => Box::new(iter::once(Err(error))) as SelectedIter<'a>,
    });

    Ok(SelectedRows {
        sources,
        rows: Box::new(rows),
    })
}
