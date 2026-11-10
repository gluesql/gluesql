use {
    super::{LabeledRows, QueryIter, order_by, project},
    crate::{
        executor::context::{RowContext, StatementContext},
        plan::{DistinctInputPlan, DistinctPlan},
        result::Result,
        store::GStore,
    },
    std::{collections::HashSet, rc::Rc},
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    filter_context: Option<Rc<RowContext<'a>>>,
    plan: &'a DistinctPlan,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let LabeledRows { labels, rows } = match &plan.input {
        DistinctInputPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, stmt_context, filter_context, project)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        DistinctInputPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, stmt_context, filter_context, order_by)
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
        rows: Box::new(rows) as QueryIter<'a>,
    })
}
