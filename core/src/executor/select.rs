mod aggregation_node;
mod distinct_node;
mod error;
mod having_node;
mod limit_node;
mod offset_node;
mod order_by;
mod project_node;
mod select_node;
mod select_order_by_node;
mod values_node;
mod values_order_by_node;

use {
    crate::{
        data::Row, executor::context::RowContext, plan::QueryPlan, result::Result, store::GStore,
    },
    std::rc::Rc,
};
pub use {error::SelectError, select_order_by_node::SortError};

pub type SelectIter<'a> = Box<dyn Iterator<Item = Result<Row>> + 'a>;

struct LabeledRows<'a> {
    labels: Vec<String>,
    rows: SelectIter<'a>,
}

fn execute<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    match query {
        QueryPlan::Project(project) => {
            let project_node::ProjectedRows { labels, rows, .. } =
                project_node::execute(storage, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        QueryPlan::Values(values) => values_node::execute(values),
        QueryPlan::SelectOrderBy(order_by) => {
            select_order_by_node::execute(storage, order_by, filter_context)
        }
        QueryPlan::ValuesOrderBy(order_by) => values_order_by_node::execute(order_by),
        QueryPlan::Distinct(distinct) => distinct_node::execute(storage, distinct, filter_context),
        QueryPlan::Offset(offset) => offset_node::execute(storage, offset, filter_context),
        QueryPlan::Limit(limit) => limit_node::execute(storage, limit, filter_context),
    }
}

pub fn select_with_labels<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<(Vec<String>, SelectIter<'a>)>
where
    T: GStore,
{
    execute(storage, query, filter_context).map(|LabeledRows { labels, rows }| (labels, rows))
}

pub fn select<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<SelectIter<'a>>
where
    T: GStore,
{
    execute(storage, query, filter_context).map(|LabeledRows { rows, .. }| rows)
}
