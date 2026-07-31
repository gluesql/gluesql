use {
    super::{JoinCandidates, hash, nested_loop},
    crate::{
        executor::{context::RowContext, filter::check_expr},
        plan::{JoinConditionInputPlan, JoinConditionPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    plan: &'a JoinConditionPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<JoinCandidates<'a>> {
    let JoinConditionPlan { input, expr } = plan;
    let JoinCandidates {
        sources,
        right,
        groups,
    } = match input {
        JoinConditionInputPlan::NestedLoop(join) => {
            nested_loop::execute(storage, join, filter_context)?
        }
        JoinConditionInputPlan::Hash(join) => hash::execute(storage, join, filter_context)?,
    };
    let filter_context = filter_context.map(Rc::clone);
    let groups = groups.map(move |group| {
        let mut group = group?;
        let filter_context = filter_context.as_ref().map(Rc::clone);
        group.rows = Box::new(group.rows.filter_map(move |row| {
            let row = match row {
                Ok(row) => row,
                Err(error) => return Some(Err(error)),
            };
            let evaluation_context = match &filter_context {
                Some(filter_context) => Some(Rc::new(RowContext::concat(
                    Rc::clone(&row),
                    Rc::clone(filter_context),
                ))),
                None => Some(Rc::clone(&row)),
            };

            match check_expr(storage, evaluation_context.as_ref(), None, expr) {
                Ok(true) => Some(Ok(row)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            }
        }));

        Ok(group)
    });

    Ok(JoinCandidates {
        sources,
        right,
        groups: Box::new(groups),
    })
}
