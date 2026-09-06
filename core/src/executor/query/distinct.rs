use {
    super::{LabeledRows, QueryIter, order_by, project},
    crate::{
        executor::context::RowContext,
        plan::{DistinctInputPlan, DistinctPlan},
        result::Result,
        store::GStore,
    },
    std::{collections::HashSet, rc::Rc},
};

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        name = "gluesql.query.distinct",
        target = "gluesql",
        level = "debug",
        skip_all,
        fields(buffered_rows = tracing::field::Empty)
    )
)]
pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a DistinctPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let LabeledRows { labels, rows } = match &plan.input {
        DistinctInputPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        DistinctInputPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, order_by, filter_context)
        }
    }?;
    let rows = rows.collect::<Result<Vec<_>>>()?;
    #[cfg(feature = "tracing")]
    tracing::Span::current().record("buffered_rows", rows.len());

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
