mod aggregation;
mod distinct;
mod error;
mod filter;
mod having;
mod join;
mod limit;
mod offset;
mod order_by;
mod output;
mod project;
mod source;
mod values;

pub use error::QueryError;
pub(super) use output::{OutputBody, body as output_body};
use {
    crate::{
        data::Row,
        executor::context::{RowContext, StatementContext},
        plan::QueryPlan,
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

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
    stmt_context: StatementContext<'a>,
    filter_context: Option<Rc<RowContext<'a>>>,
    query: &'a QueryPlan,
) -> Result<(Vec<String>, QueryIter<'a>)>
where
    T: GStore,
{
    execute_query(storage, stmt_context, filter_context, query)
        .map(|LabeledRows { labels, rows }| (labels, rows))
}

pub fn execute<'a, T>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    filter_context: Option<Rc<RowContext<'a>>>,
    query: &'a QueryPlan,
) -> Result<QueryIter<'a>>
where
    T: GStore,
{
    execute_query(storage, stmt_context, filter_context, query).map(|LabeledRows { rows, .. }| rows)
}

fn execute_query<'a, T>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    filter_context: Option<Rc<RowContext<'a>>>,
    query: &'a QueryPlan,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    match query {
        QueryPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, stmt_context, filter_context, project)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        QueryPlan::Values(values) => {
            values::execute(storage, stmt_context, filter_context.as_ref(), values)
        }
        QueryPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, stmt_context, filter_context, order_by)
        }
        QueryPlan::ValuesOrderBy(order_by) => {
            order_by::values::execute(storage, stmt_context, filter_context.as_ref(), order_by)
        }
        QueryPlan::Distinct(distinct) => {
            distinct::execute(storage, stmt_context, filter_context, distinct)
        }
        QueryPlan::Offset(offset) => offset::execute(storage, stmt_context, filter_context, offset),
        QueryPlan::Limit(limit) => limit::execute(storage, stmt_context, filter_context, limit),
    }
}
