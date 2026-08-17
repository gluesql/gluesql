use {
    gluesql_core::{
        ast::*,
        plan::{
            AggregationInputPlan, AggregationPlan, ExprPlan, HavingPlan, ProjectInputPlan,
            ProjectPlan, QueryPlan, SourcePlan, StatementPlan, TableAccessPlan,
        },
        prelude::{Glue, Payload, Result},
        store::{GStore, GStoreMut, Planner},
    },
    pretty_assertions::assert_eq,
};

pub mod macros;

pub fn test_indexes(statement: &StatementPlan, indexes: Option<Vec<TableAccessPlan>>) {
    if let Some(expected) = indexes {
        let found = find_indexes(statement);

        assert!(
            expected.len() == found.len(),
            "num of indexes does not match: found({}) != expected({})",
            found.len(),
            expected.len(),
        );

        for expected_index in expected {
            assert!(
                found.contains(&(&expected_index)),
                "index does not exist: {expected_index:#?}"
            );
        }
    }
}

fn find_indexes(statement: &StatementPlan) -> Vec<&TableAccessPlan> {
    fn find_expr_indexes(expr: &ExprPlan) -> Vec<&TableAccessPlan> {
        match expr {
            ExprPlan::Subquery(query)
            | ExprPlan::Exists {
                subquery: query, ..
            }
            | ExprPlan::InSubquery {
                subquery: query, ..
            } => find_query_indexes(query),
            _ => Vec::new(),
        }
    }

    fn find_source_indexes(source: &SourcePlan) -> Vec<&TableAccessPlan> {
        match source {
            // `FullScanRequired` is a planner marker, not an access path, so it is not an index.
            SourcePlan::Table(table)
                if !matches!(
                    table.access,
                    TableAccessPlan::FullScan | TableAccessPlan::FullScanRequired
                ) =>
            {
                vec![&table.access]
            }
            SourcePlan::Derived(derived) => find_query_indexes(&derived.query),
            SourcePlan::Table(_) | SourcePlan::Series(_) | SourcePlan::Dictionary(_) => Vec::new(),
        }
    }

    fn find_project_indexes(project: &ProjectPlan) -> Vec<&TableAccessPlan> {
        let filter = match &project.input {
            ProjectInputPlan::Filter(filter)
            | ProjectInputPlan::Aggregation(AggregationPlan {
                input: AggregationInputPlan::Filter(filter),
                ..
            })
            | ProjectInputPlan::Having(HavingPlan {
                input:
                    AggregationPlan {
                        input: AggregationInputPlan::Filter(filter),
                        ..
                    },
                ..
            }) => Some(filter),
            ProjectInputPlan::Source(_)
            | ProjectInputPlan::InnerJoin(_)
            | ProjectInputPlan::LeftOuterJoin(_)
            | ProjectInputPlan::UnplannedRightOuterJoin(_)
            | ProjectInputPlan::RightOuterJoin(_)
            | ProjectInputPlan::Aggregation(_)
            | ProjectInputPlan::Having(_) => None,
        };
        let filter_indexes = filter
            .map(|filter| find_expr_indexes(&filter.expr))
            .unwrap_or_default();
        let source_indexes = find_source_indexes(project.input.base_source());

        [filter_indexes, source_indexes].concat()
    }

    fn find_query_indexes(query: &QueryPlan) -> Vec<&TableAccessPlan> {
        query
            .project()
            .map(find_project_indexes)
            .unwrap_or_default()
    }

    match statement {
        StatementPlan::Query(query) => find_query_indexes(query),
        _ => vec![],
    }
}

pub fn type_match(expected: &[DataType], found: Result<Payload>) {
    let Ok(Payload::Select {
        labels: _expected_labels,
        rows,
    }) = found
    else {
        panic!("type match is only for Select")
    };

    for (i, items) in rows.iter().enumerate() {
        assert_eq!(
            items.len(),
            expected.len(),
            "\n[err: size of row] row index: {}\n found: {:?}\n expected: {:?}",
            i,
            items.len(),
            expected.len()
        );

        items
            .iter()
            .zip(expected.iter())
            .for_each(|(value, data_type)| match value.validate_type(data_type) {
                Ok(()) => {}
                Err(e) => {
                    panic!("[err: type match failed]\n found {value:?}\n expected {data_type:?}\n error: {e:?}\n")
                }
            });
    }
}

/// If you want to make your custom storage and want to run integrate tests,
/// you should implement this `Tester` trait.
///
/// To see how to use it,
/// * [tests/memory_storage.rs](https://github.com/gluesql/gluesql/blob/main/storages/memory-storage/tests/memory_storage.rs)
/// * [tests/sled_storage.rs](https://github.com/gluesql/gluesql/blob/main/storages/sled-storage/tests/sled_storage.rs)
///
/// Actual test cases are in [test-suite/src/](https://github.com/gluesql/gluesql/blob/main/test-suite/src/),
/// not in `/tests/`.
pub trait Tester<T: GStore + GStoreMut + Planner> {
    fn new(namespace: &str) -> Self;

    fn get_glue(&mut self) -> &mut Glue<T>;
}

#[macro_export]
macro_rules! test_case {
    ($name: ident, $content: expr) => {
        pub fn $name<T>(mut tester: impl $crate::Tester<T>)
        where
            T: gluesql_core::store::GStore
                + gluesql_core::store::GStoreMut
                + gluesql_core::store::Planner,
        {
            #[allow(unused_variables)]
            let glue = tester.get_glue();

            #[allow(unused_macros)]
            macro_rules! get_glue {
                () => {
                    glue
                };
            }

            $content;

            gluesql_core::prelude::Result::<()>::Ok(()).unwrap()
        }
    };
}
