use {
    super::{
        LabeledRows, distinct_node, project_node, select_order_by_node, values_node,
        values_order_by_node,
    },
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
            let project_node::ProjectedRows { labels, rows, .. } =
                project_node::execute(storage, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        OffsetInputPlan::Values(values) => values_node::execute(values),
        OffsetInputPlan::SelectOrderBy(order_by) => {
            select_order_by_node::execute(storage, order_by, filter_context)
        }
        OffsetInputPlan::ValuesOrderBy(order_by) => values_order_by_node::execute(order_by),
        OffsetInputPlan::Distinct(distinct) => {
            distinct_node::execute(storage, distinct, filter_context)
        }
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
