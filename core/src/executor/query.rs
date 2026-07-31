mod aggregation_node;
mod distinct_node;
mod error;
mod filter_node;
mod hash_join_node;
mod having_node;
mod inner_join_node;
mod join_condition_node;
mod left_outer_join_node;
mod limit_node;
mod nested_loop_join_node;
mod offset_node;
mod order_by;
mod project_node;
mod projection_labels;
mod select_order_by_node;
mod source_node;
mod values_node;
mod values_order_by_node;

use {
    crate::{
        data::Row, executor::context::RowContext, plan::QueryPlan, result::Result, store::GStore,
    },
    std::rc::Rc,
};
pub use {error::SelectError, select_order_by_node::SortError};

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

struct JoinCandidateGroup<'a> {
    left: Rc<RowContext<'a>>,
    rows: SelectedIter<'a>,
}

type JoinCandidateGroupIter<'a> = Box<dyn Iterator<Item = Result<JoinCandidateGroup<'a>>> + 'a>;

struct JoinCandidates<'a> {
    sources: SelectedSources<'a>,
    right: SourceColumns<'a>,
    groups: JoinCandidateGroupIter<'a>,
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
