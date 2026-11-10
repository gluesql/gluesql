use {
    super::{LabeledRows, distinct, order_by, project, values},
    crate::{
        data::Value,
        executor::{
            context::{RowContext, StatementContext},
            evaluate::evaluate,
        },
        plan::{ExprPlan, OffsetInputPlan, OffsetPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    filter_context: Option<Rc<RowContext<'a>>>,
    plan: &'a OffsetPlan,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let count_context = filter_context.as_ref().map(Rc::clone);
    let LabeledRows { labels, rows } = match &plan.input {
        OffsetInputPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, stmt_context, filter_context, project)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        OffsetInputPlan::Values(values_plan) => {
            values::execute(storage, stmt_context, filter_context.as_ref(), values_plan)
        }
        OffsetInputPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, stmt_context, filter_context, order_by)
        }
        OffsetInputPlan::ValuesOrderBy(order_by) => {
            order_by::values::execute(storage, stmt_context, filter_context.as_ref(), order_by)
        }
        OffsetInputPlan::Distinct(distinct) => {
            distinct::execute(storage, stmt_context, filter_context, distinct)
        }
    }?;
    let count = evaluate_count(storage, stmt_context, count_context.as_ref(), &plan.count)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.skip(count)),
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
