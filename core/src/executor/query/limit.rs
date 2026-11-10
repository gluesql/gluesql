use {
    super::{LabeledRows, distinct, offset, order_by, project, values},
    crate::{
        data::Value,
        executor::{
            context::{RowContext, StatementContext},
            evaluate::evaluate,
        },
        plan::{ExprPlan, LimitInputPlan, LimitPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    filter_context: Option<Rc<RowContext<'a>>>,
    plan: &'a LimitPlan,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let count_context = filter_context.as_ref().map(Rc::clone);
    let LabeledRows { labels, rows } = match &plan.input {
        LimitInputPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, stmt_context, filter_context, project)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        LimitInputPlan::Values(values_plan) => {
            values::execute(storage, stmt_context, filter_context.as_ref(), values_plan)
        }
        LimitInputPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, stmt_context, filter_context, order_by)
        }
        LimitInputPlan::ValuesOrderBy(order_by) => {
            order_by::values::execute(storage, stmt_context, filter_context.as_ref(), order_by)
        }
        LimitInputPlan::Distinct(distinct) => {
            distinct::execute(storage, stmt_context, filter_context, distinct)
        }
        LimitInputPlan::Offset(offset_plan) => {
            offset::execute(storage, stmt_context, filter_context, offset_plan)
        }
    }?;
    let count = evaluate_count(storage, stmt_context, count_context.as_ref(), &plan.count)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.take(count)),
    })
}

fn evaluate_count<T: GStore>(
    storage: &T,
    stmt_context: StatementContext<'_>,
    context: Option<&Rc<RowContext<'_>>>,
    expr: &ExprPlan,
) -> Result<usize> {
    let evaluated = evaluate(storage, stmt_context, context, None, expr)?;
    let size: usize = Value::try_from(evaluated)?.try_into()?;

    Ok(size)
}
