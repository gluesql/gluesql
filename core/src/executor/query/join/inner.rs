use {
    super::super::{SelectedIter, SelectedRows},
    super::{JoinCandidates, condition, hash, nested_loop},
    crate::{
        executor::context::{RowContext, StatementContext},
        plan::{InnerJoinInputPlan, InnerJoinPlan},
        result::Result,
        store::GStore,
    },
    std::{iter, rc::Rc},
};

pub(crate) fn execute<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    filter_context: Option<&Rc<RowContext<'a>>>,
    plan: &'a InnerJoinPlan,
) -> Result<SelectedRows<'a>> {
    let JoinCandidates {
        sources, groups, ..
    } = match &plan.input {
        InnerJoinInputPlan::NestedLoop(join) => {
            nested_loop::execute(storage, stmt_context, filter_context, join)?
        }
        InnerJoinInputPlan::Hash(join) => {
            hash::execute(storage, stmt_context, filter_context, join)?
        }
        InnerJoinInputPlan::Condition(condition) => {
            condition::execute(storage, stmt_context, filter_context, condition)?
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
