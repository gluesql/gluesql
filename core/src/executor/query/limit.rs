use {
    super::{LabeledRows, distinct, offset, order_by, project, values},
    crate::{
        data::Value,
        executor::{context::RowContext, evaluate::evaluate_stateless},
        plan::{ExprPlan, LimitInputPlan, LimitPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a LimitPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let LabeledRows { labels, rows } = match &plan.input {
        LimitInputPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        LimitInputPlan::Values(values_plan) => values::execute(values_plan),
        LimitInputPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, order_by, filter_context)
        }
        LimitInputPlan::ValuesOrderBy(order_by) => order_by::values::execute(order_by),
        LimitInputPlan::Distinct(distinct) => distinct::execute(storage, distinct, filter_context),
        LimitInputPlan::Offset(offset_plan) => {
            offset::execute(storage, offset_plan, filter_context)
        }
    }?;
    let count = evaluate_count(&plan.count)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.take(count)),
    })
}

fn evaluate_count(expr: &ExprPlan) -> Result<usize> {
    let evaluated = evaluate_stateless(None, expr)?;
    let size: usize = Value::try_from(evaluated)?.try_into()?;

    Ok(size)
}
