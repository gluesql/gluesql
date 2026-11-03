use {
    super::{LabeledRows, distinct, order_by, project, values},
    crate::{
        data::Value,
        executor::{context::RowContext, evaluate::evaluate},
        plan::{CustomFunctionPlan, ExprPlan, OffsetInputPlan, OffsetPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    functions: &'a [CustomFunctionPlan],
    plan: &'a OffsetPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let count_context = filter_context.as_ref().map(Rc::clone);
    let LabeledRows { labels, rows } = match &plan.input {
        OffsetInputPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, functions, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        OffsetInputPlan::Values(values_plan) => {
            values::execute(storage, functions, values_plan, filter_context.as_ref())
        }
        OffsetInputPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, functions, order_by, filter_context)
        }
        OffsetInputPlan::ValuesOrderBy(order_by) => {
            order_by::values::execute(storage, functions, order_by, filter_context.as_ref())
        }
        OffsetInputPlan::Distinct(distinct) => {
            distinct::execute(storage, functions, distinct, filter_context)
        }
    }?;
    let count = evaluate_count(storage, functions, count_context.as_ref(), &plan.count)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.skip(count)),
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
