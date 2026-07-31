mod aggregation_node;
mod distinct_node;
mod error;
mod filter_node;
mod having_node;
mod join_node;
mod limit_node;
mod offset_node;
mod order_by;
mod project_node;
mod select_order_by_node;
mod source_node;
mod values_node;
mod values_order_by_node;

use {
    crate::{
        data::Row,
        executor::context::RowContext,
        plan::{
            DistinctInputPlan, DistinctPlan, LimitInputPlan, LimitPlan, OffsetInputPlan,
            OffsetPlan, QueryPlan,
        },
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};
pub use {error::SelectError, select_order_by_node::SortError};

pub type SelectIter<'a> = Box<dyn Iterator<Item = Result<Row>> + 'a>;
type SelectedRows<'a> = Box<dyn Iterator<Item = Result<Rc<RowContext<'a>>>> + 'a>;

struct LabeledRows<'a> {
    labels: Vec<String>,
    rows: SelectIter<'a>,
}

fn labels<T: GStore>(storage: &T, query: &QueryPlan) -> Result<Vec<String>> {
    match query {
        QueryPlan::Project(project) => project_node::labels(storage, project),
        QueryPlan::Values(values) => Ok(values_node::labels(values)),
        QueryPlan::SelectOrderBy(order_by) => project_node::labels(storage, &order_by.input),
        QueryPlan::ValuesOrderBy(order_by) => Ok(values_node::labels(&order_by.input)),
        QueryPlan::Distinct(distinct) => distinct_labels(storage, distinct),
        QueryPlan::Offset(offset) => offset_labels(storage, offset),
        QueryPlan::Limit(limit) => limit_labels(storage, limit),
    }
}

fn distinct_labels<T: GStore>(storage: &T, distinct: &DistinctPlan) -> Result<Vec<String>> {
    match &distinct.input {
        DistinctInputPlan::Project(project) => project_node::labels(storage, project),
        DistinctInputPlan::SelectOrderBy(order_by) => {
            project_node::labels(storage, &order_by.input)
        }
    }
}

fn offset_labels<T: GStore>(storage: &T, offset: &OffsetPlan) -> Result<Vec<String>> {
    match &offset.input {
        OffsetInputPlan::Project(project) => project_node::labels(storage, project),
        OffsetInputPlan::Values(values) => Ok(values_node::labels(values)),
        OffsetInputPlan::SelectOrderBy(order_by) => project_node::labels(storage, &order_by.input),
        OffsetInputPlan::ValuesOrderBy(order_by) => Ok(values_node::labels(&order_by.input)),
        OffsetInputPlan::Distinct(distinct) => distinct_labels(storage, distinct),
    }
}

fn limit_labels<T: GStore>(storage: &T, limit: &LimitPlan) -> Result<Vec<String>> {
    match &limit.input {
        LimitInputPlan::Project(project) => project_node::labels(storage, project),
        LimitInputPlan::Values(values) => Ok(values_node::labels(values)),
        LimitInputPlan::SelectOrderBy(order_by) => project_node::labels(storage, &order_by.input),
        LimitInputPlan::ValuesOrderBy(order_by) => Ok(values_node::labels(&order_by.input)),
        LimitInputPlan::Distinct(distinct) => distinct_labels(storage, distinct),
        LimitInputPlan::Offset(offset) => offset_labels(storage, offset),
    }
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
