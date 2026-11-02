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
        executor::context::{ExecutionContext, RowContext},
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
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<(Vec<String>, QueryIter<'a>)>
where
    T: GStore,
{
    let execution_context = ExecutionContext::new();
    let result = {
        let _scope = execution_context.activate();
        execute_query(storage, query, filter_context)
    };
    result.map(|LabeledRows { labels, rows }| {
        (labels, keep_execution_context(rows, execution_context))
    })
}

pub fn execute<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<QueryIter<'a>>
where
    T: GStore,
{
    let execution_context = ExecutionContext::new();
    let result = {
        let _scope = execution_context.activate();
        execute_query(storage, query, filter_context)
    };
    result.map(|LabeledRows { rows, .. }| keep_execution_context(rows, execution_context))
}

fn keep_execution_context(
    rows: QueryIter<'_>,
    execution_context: Rc<ExecutionContext>,
) -> QueryIter<'_> {
    Box::new(ExecutionScopedIter {
        rows,
        execution_context,
    })
}

struct ExecutionScopedIter<'a> {
    rows: QueryIter<'a>,
    execution_context: Rc<ExecutionContext>,
}

impl Iterator for ExecutionScopedIter<'_> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        let _scope = self.execution_context.activate();
        self.rows.next()
    }
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

#[cfg(test)]
mod tests {
    use {
        super::{QueryIter, execute, execute_with_labels, keep_execution_context},
        crate::{
            data::{Row, Value, regex_with_cache},
            mock::MockStorage,
            plan::{ExprPlan, QueryPlan, ValuesPlan},
        },
        std::rc::Rc,
    };

    #[test]
    fn execute_keeps_context_alive_until_rows_are_consumed() {
        let query = QueryPlan::Values(ValuesPlan(vec![vec![ExprPlan::Regex {
            expr: Box::new(ExprPlan::Value(Value::Str("Hello".to_owned()))),
            negated: false,
            pattern: Box::new(ExprPlan::Value(Value::Str("^Hello$".to_owned()))),
            case_sensitive: true,
        }]]));
        let rows = execute(&MockStorage::default(), &query, None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![Value::Bool(true)]);
    }

    #[test]
    fn execute_with_labels_keeps_context_alive_until_rows_are_consumed() {
        let query = QueryPlan::Values(ValuesPlan(vec![vec![ExprPlan::Value(Value::Str(
            "Hello".to_owned(),
        ))]]));
        let storage = MockStorage::default();
        let (labels, rows) = execute_with_labels(&storage, &query, None).unwrap();

        assert_eq!(labels, vec!["column1"]);
        assert_eq!(rows.count(), 1);
    }

    #[test]
    fn interleaved_queries_keep_execution_caches_separate() {
        let query = QueryPlan::Values(ValuesPlan(vec![vec![ExprPlan::Like {
            expr: Box::new(ExprPlan::Value(Value::Str("Hello".to_owned()))),
            negated: false,
            pattern: Box::new(ExprPlan::Value(Value::Str("H%".to_owned()))),
        }]]));
        let storage = MockStorage::default();
        let mut first = execute(&storage, &query, None).unwrap();
        let mut second = execute(&storage, &query, None).unwrap();

        assert_eq!(
            first.next().unwrap().unwrap().values,
            vec![Value::Bool(true)]
        );
        assert_eq!(
            second.next().unwrap().unwrap().values,
            vec![Value::Bool(true)]
        );
        assert!(first.next().is_none());
        assert!(second.next().is_none());
    }

    #[test]
    fn values_query_reuses_pattern_cache_across_rows() {
        let value = |value: &str| ExprPlan::Value(Value::Str(value.to_owned()));
        let pattern = || ExprPlan::Like {
            expr: Box::new(value("Hello")),
            negated: false,
            pattern: Box::new(value("H%")),
        };
        let query = QueryPlan::Values(ValuesPlan(vec![
            vec![pattern()],
            vec![pattern()],
            vec![pattern()],
        ]));
        let storage = MockStorage::default();
        let execution_context = super::ExecutionContext::new();
        let _scope = execution_context.activate();

        let rows = super::execute_query(&storage, &query, None)
            .unwrap()
            .rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 3);
        super::ExecutionContext::with_regex_cache(|cache| assert_eq!(cache.len(), 1));
    }

    #[test]
    fn interleaved_iterators_activate_their_own_regex_caches() {
        let first_context = super::ExecutionContext::new();
        let second_context = super::ExecutionContext::new();
        {
            let _scope = first_context.activate();
            super::ExecutionContext::with_regex_cache(|cache| {
                regex_with_cache("first", "first", true, cache).unwrap();
            });
        }
        {
            let _scope = second_context.activate();
            super::ExecutionContext::with_regex_cache(|cache| {
                regex_with_cache("second", "second", true, cache).unwrap();
                regex_with_cache("other", "other", true, cache).unwrap();
            });
        }

        let cache_size_row = || {
            Box::new(std::iter::from_fn({
                let mut yielded = false;
                move || {
                    if yielded {
                        return None;
                    }
                    yielded = true;
                    let size = super::ExecutionContext::with_regex_cache(|cache| cache.len());
                    Some(Ok(Row {
                        columns: Rc::from(["cache_size".to_owned()]),
                        values: vec![Value::I64(size as i64)],
                    }))
                }
            })) as QueryIter<'_>
        };
        let mut first = keep_execution_context(cache_size_row(), first_context);
        let mut second = keep_execution_context(cache_size_row(), second_context);

        assert_eq!(first.next().unwrap().unwrap().values, vec![Value::I64(1)]);
        assert_eq!(second.next().unwrap().unwrap().values, vec![Value::I64(2)]);
        assert!(first.next().is_none());
        assert!(second.next().is_none());
    }
}
