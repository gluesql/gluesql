use {
    super::{LabeledRows, distinct, order_by, project, values},
    crate::{
        data::Value,
        executor::{context::RowContext, evaluate::evaluate_stateless},
        plan::{ExprPlan, OffsetInputPlan, OffsetPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a OffsetPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let LabeledRows { labels, rows } = match &plan.input {
        OffsetInputPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        OffsetInputPlan::Values(values_plan) => values::execute(values_plan),
        OffsetInputPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, order_by, filter_context)
        }
        OffsetInputPlan::ValuesOrderBy(order_by) => order_by::values::execute(order_by),
        OffsetInputPlan::Distinct(distinct) => distinct::execute(storage, distinct, filter_context),
    }?;
    let count = evaluate_count(&plan.count)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.skip(count)),
    })
}

fn evaluate_count(expr: &ExprPlan) -> Result<usize> {
    let evaluated = evaluate_stateless(None, expr)?;
    let size: usize = Value::try_from(evaluated)?.try_into()?;

    Ok(size)
}
