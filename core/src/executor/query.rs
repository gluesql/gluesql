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
        executor::context::RowContext,
        plan::{CustomFunctionPlan, QueryPlan},
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
    functions: &'a [CustomFunctionPlan],
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<(Vec<String>, QueryIter<'a>)>
where
    T: GStore,
{
    execute_query(storage, functions, query, filter_context)
        .map(|LabeledRows { labels, rows }| (labels, rows))
}

pub fn execute<'a, T>(
    storage: &'a T,
    functions: &'a [CustomFunctionPlan],
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<QueryIter<'a>>
where
    T: GStore,
{
    execute_query(storage, functions, query, filter_context).map(|LabeledRows { rows, .. }| rows)
}

fn execute_query<'a, T>(
    storage: &'a T,
    functions: &'a [CustomFunctionPlan],
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    match query {
        QueryPlan::Project(project) => {
            let project::ProjectedRows { labels, rows, .. } =
                project::execute(storage, functions, project, filter_context)?;
            let rows = rows.map(|row| row.map(|(.., row)| row));

            Ok(LabeledRows {
                labels,
                rows: Box::new(rows),
            })
        }
        QueryPlan::Values(values) => {
            values::execute(storage, functions, values, filter_context.as_ref())
        }
        QueryPlan::SelectOrderBy(order_by) => {
            order_by::select::execute(storage, functions, order_by, filter_context)
        }
        QueryPlan::ValuesOrderBy(order_by) => {
            order_by::values::execute(storage, functions, order_by, filter_context.as_ref())
        }
        QueryPlan::Distinct(distinct) => {
            distinct::execute(storage, functions, distinct, filter_context)
        }
        QueryPlan::Offset(offset) => offset::execute(storage, functions, offset, filter_context),
        QueryPlan::Limit(limit) => limit::execute(storage, functions, limit, filter_context),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::execute,
        crate::{
            data::{Row, Value},
            mock::MockStorage,
            parse_sql::parse_query,
            plan::QueryPlan,
            result::Result,
            translate::{NO_PARAMS, translate_query},
        },
        std::rc::Rc,
    };

    #[test]
    fn values_query_uses_outer_scope_for_order_and_bounds() {
        let storage = MockStorage::default();
        let query = QueryPlan::from(
            translate_query(
                &parse_query("VALUES (x), (x + 1) ORDER BY column1 + x DESC LIMIT x - 3 OFFSET 0")
                    .unwrap(),
                NO_PARAMS,
            )
            .unwrap(),
        );
        let arguments = Row {
            columns: vec!["x".to_owned()].into(),
            values: vec![Value::I64(4)],
        };
        let arguments = Rc::new(arguments.as_context());
        let rows = execute(&storage, &[], &query, Some(arguments))
            .unwrap()
            .map(|row| row.map(Row::into_values))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows, vec![vec![Value::I64(5)]]);
    }
}
