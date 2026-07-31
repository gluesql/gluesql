use {
    gluesql_core::{
        ast::*,
        plan::{
            DistinctInputPlan, DistinctPlan, IndexItemPlan, LimitInputPlan, LimitPlan,
            OffsetInputPlan, OffsetPlan, ProjectInputPlan, ProjectPlan, QueryPlan, SelectPlan,
            StatementPlan,
        },
        prelude::{Glue, Payload, Result},
        store::{GStore, GStoreMut, Planner},
    },
    pretty_assertions::assert_eq,
};

pub mod macros;

pub fn test_indexes(statement: &StatementPlan, indexes: Option<Vec<IndexItemPlan>>) {
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

fn find_indexes(statement: &StatementPlan) -> Vec<&IndexItemPlan> {
    fn find_expr_indexes(expr: &gluesql_core::plan::ExprPlan) -> Vec<&IndexItemPlan> {
        match expr {
            gluesql_core::plan::ExprPlan::Subquery(query)
            | gluesql_core::plan::ExprPlan::Exists {
                subquery: query, ..
            }
            | gluesql_core::plan::ExprPlan::InSubquery {
                subquery: query, ..
            } => find_query_indexes(query),
            _ => vec![],
        }
    }

    fn find_select_indexes(select: &SelectPlan) -> Vec<&IndexItemPlan> {
        let selection_indexes = select
            .selection
            .as_ref()
            .map(find_expr_indexes)
            .unwrap_or_default();

        let table_indexes = match &select.from.relation {
            gluesql_core::plan::TableFactorPlan::Table {
                index: Some(index), ..
            } => vec![index],
            _ => vec![],
        };

        [selection_indexes, table_indexes].concat()
    }

    fn find_offset_indexes(offset: &OffsetPlan) -> Vec<&IndexItemPlan> {
        match &offset.input {
            OffsetInputPlan::Project(project) => find_project_indexes(project),
            OffsetInputPlan::Values(_) | OffsetInputPlan::ValuesOrderBy(_) => Vec::new(),
            OffsetInputPlan::SelectOrderBy(order_by) => find_project_indexes(&order_by.input),
            OffsetInputPlan::Distinct(distinct) => find_distinct_indexes(distinct),
        }
    }

    fn find_project_indexes(project: &ProjectPlan) -> Vec<&IndexItemPlan> {
        let select = match &project.input {
            ProjectInputPlan::Select(select) => select.as_ref(),
            ProjectInputPlan::Aggregation(aggregation) => aggregation.input.as_ref(),
            ProjectInputPlan::Having(having) => having.input.input.as_ref(),
        };

        find_select_indexes(select)
    }

    fn find_distinct_indexes(distinct: &DistinctPlan) -> Vec<&IndexItemPlan> {
        match &distinct.input {
            DistinctInputPlan::Project(project) => find_project_indexes(project),
            DistinctInputPlan::SelectOrderBy(order_by) => find_project_indexes(&order_by.input),
        }
    }

    fn find_query_indexes(query: &QueryPlan) -> Vec<&IndexItemPlan> {
        match query {
            QueryPlan::Project(project) => find_project_indexes(project),
            QueryPlan::Values(_) | QueryPlan::ValuesOrderBy(_) => Vec::new(),
            QueryPlan::SelectOrderBy(order_by) => find_project_indexes(&order_by.input),
            QueryPlan::Distinct(distinct) => find_distinct_indexes(distinct),
            QueryPlan::Offset(offset) => find_offset_indexes(offset),
            QueryPlan::Limit(LimitPlan { input, .. }) => match input {
                LimitInputPlan::Project(project) => find_project_indexes(project),
                LimitInputPlan::Values(_) | LimitInputPlan::ValuesOrderBy(_) => Vec::new(),
                LimitInputPlan::SelectOrderBy(order_by) => find_project_indexes(&order_by.input),
                LimitInputPlan::Distinct(distinct) => find_distinct_indexes(distinct),
                LimitInputPlan::Offset(offset) => find_offset_indexes(offset),
            },
        }
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
