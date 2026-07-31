mod aggregation;
mod distinct;
mod error;
mod filter;
mod having;
mod join;
mod limit;
mod offset;
mod order_by;
mod project;
mod source;
mod values;

use {
    crate::{
        data::Row, executor::context::RowContext, plan::QueryPlan, result::Result, store::GStore,
    },
    std::rc::Rc,
};
pub use {error::SelectError, order_by::SortError};

pub type QueryIter<'a> = Box<dyn Iterator<Item = Result<Row>> + 'a>;
type SelectedIter<'a> = Box<dyn Iterator<Item = Result<Rc<RowContext<'a>>>> + 'a>;

struct SourceColumns<'a> {
    alias: &'a str,
    names: Rc<[String]>,
}

struct SelectedSources<'a> {
    base: SourceColumns<'a>,
    joined: Vec<SourceColumns<'a>>,
}

struct SelectedRows<'a> {
    sources: SelectedSources<'a>,
    rows: SelectedIter<'a>,
}

struct LabeledRows<'a> {
    labels: Vec<String>,
    rows: QueryIter<'a>,
}

pub fn execute_with_labels<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<(Vec<String>, QueryIter<'a>)>
where
    T: GStore,
{
    execute_query(storage, query, filter_context).map(|LabeledRows { labels, rows }| (labels, rows))
}

pub fn execute<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<QueryIter<'a>>
where
    T: GStore,
{
    execute_query(storage, query, filter_context).map(|LabeledRows { rows, .. }| rows)
}

fn execute_query<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    match query {
        QueryPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        QueryPlan::Values(values) => values::execute(values),
        QueryPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, order_by, filter_context)
        }
        QueryPlan::ValuesOrderBy(order_by) => order_by::values::execute(order_by),
        QueryPlan::Distinct(distinct) => distinct::execute(storage, distinct, filter_context),
        QueryPlan::Offset(offset) => offset::execute(storage, offset, filter_context),
        QueryPlan::Limit(limit) => limit::execute(storage, limit, filter_context),
    }
}
