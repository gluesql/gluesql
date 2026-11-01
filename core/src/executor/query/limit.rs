use {
    super::{LabeledRows, distinct, offset, order_by, project, values},
    crate::{
        data::Value,
        executor::{context::RowContext, evaluate::evaluate},
        plan::{CustomFunctionPlan, ExprPlan, LimitInputPlan, LimitPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    functions: &'a [CustomFunctionPlan],
    plan: &'a LimitPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let count_context = filter_context.as_ref().map(Rc::clone);
    let LabeledRows { labels, rows } = match &plan.input {
        LimitInputPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, functions, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        LimitInputPlan::Values(values_plan) => {
            values::execute(storage, functions, values_plan, filter_context.as_ref())
        }
        LimitInputPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, functions, order_by, filter_context)
        }
        LimitInputPlan::ValuesOrderBy(order_by) => {
            order_by::values::execute(storage, functions, order_by, filter_context.as_ref())
        }
        LimitInputPlan::Distinct(distinct) => {
            distinct::execute(storage, functions, distinct, filter_context)
        }
        LimitInputPlan::Offset(offset_plan) => {
            offset::execute(storage, functions, offset_plan, filter_context)
        }
    }?;
    let count = evaluate_count(storage, functions, count_context.as_ref(), &plan.count)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.take(count)),
    })
}

fn evaluate_count<T: GStore>(
    storage: &T,
    functions: &[CustomFunctionPlan],
    context: Option<&Rc<RowContext<'_>>>,
    expr: &ExprPlan,
) -> Result<usize> {
    let evaluated = evaluate(storage, functions, context, None, expr)?;
    let size: usize = Value::try_from(evaluated)?.try_into()?;

    Ok(size)
}
