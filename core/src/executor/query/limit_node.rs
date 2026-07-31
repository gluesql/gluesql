use {
    super::{
        LabeledRows, distinct_node, offset_node, project_node, select_order_by_node, values_node,
        values_order_by_node,
    },
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
            let project_node::ProjectedRows { labels, rows, .. } =
                project_node::execute(storage, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        LimitInputPlan::Values(values) => values_node::execute(values),
        LimitInputPlan::SelectOrderBy(order_by) => {
            select_order_by_node::execute(storage, order_by, filter_context)
        }
        LimitInputPlan::ValuesOrderBy(order_by) => values_order_by_node::execute(order_by),
        LimitInputPlan::Distinct(distinct) => {
            distinct_node::execute(storage, distinct, filter_context)
        }
        LimitInputPlan::Offset(offset) => offset_node::execute(storage, offset, filter_context),
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
