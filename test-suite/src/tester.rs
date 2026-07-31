use {
    gluesql_core::{
        ast::*,
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, FilterInputPlan, FilterPlan,
            HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
            JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
            LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
            OffsetInputPlan, OffsetPlan, ProjectInputPlan, ProjectPlan, QueryPlan, SourcePlan,
            StatementPlan, TableAccessPlan,
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
    fn find_expr_indexes(expr: &gluesql_core::plan::ExprPlan) -> Vec<&TableAccessPlan> {
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

    fn find_source_indexes(source: &SourcePlan) -> Vec<&TableAccessPlan> {
        match source {
            SourcePlan::Table(table) if table.access != TableAccessPlan::FullScan => {
                vec![&table.access]
            }
            SourcePlan::Derived(derived) => find_query_indexes(&derived.query),
            SourcePlan::Table(_) | SourcePlan::Series(_) | SourcePlan::Dictionary(_) => Vec::new(),
        }
    }

    fn find_inner_join_indexes(join: &InnerJoinPlan) -> Vec<&TableAccessPlan> {
        match &join.input {
            InnerJoinInputPlan::NestedLoop(join) => find_nested_loop_indexes(join),
            InnerJoinInputPlan::Hash(join) => find_hash_indexes(join),
            InnerJoinInputPlan::Condition(condition) => find_condition_indexes(condition),
        }
    }

    fn find_left_outer_join_indexes(join: &LeftOuterJoinPlan) -> Vec<&TableAccessPlan> {
        match &join.input {
            LeftOuterJoinInputPlan::NestedLoop(join) => find_nested_loop_indexes(join),
            LeftOuterJoinInputPlan::Hash(join) => find_hash_indexes(join),
            LeftOuterJoinInputPlan::Condition(condition) => find_condition_indexes(condition),
        }
    }

    fn find_condition_indexes(condition: &JoinConditionPlan) -> Vec<&TableAccessPlan> {
        let input = match &condition.input {
            JoinConditionInputPlan::NestedLoop(join) => find_nested_loop_indexes(join),
            JoinConditionInputPlan::Hash(join) => find_hash_indexes(join),
        };

        [input, find_expr_indexes(&condition.expr)].concat()
    }

    fn find_nested_loop_indexes(join: &NestedLoopJoinPlan) -> Vec<&TableAccessPlan> {
        let input = match &join.input {
            NestedLoopJoinInputPlan::Source(source) => find_source_indexes(source),
            NestedLoopJoinInputPlan::InnerJoin(join) => find_inner_join_indexes(join),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => find_left_outer_join_indexes(join),
        };

        [input, find_source_indexes(&join.right)].concat()
    }

    fn find_hash_indexes(join: &HashJoinPlan) -> Vec<&TableAccessPlan> {
        let input = match &join.input {
            HashJoinInputPlan::Source(source) => find_source_indexes(source),
            HashJoinInputPlan::InnerJoin(join) => find_inner_join_indexes(join),
            HashJoinInputPlan::LeftOuterJoin(join) => find_left_outer_join_indexes(join),
        };
        let expressions = [
            find_expr_indexes(&join.input_key),
            find_expr_indexes(&join.right_key),
            join.right_filter
                .as_ref()
                .map_or_else(Vec::new, find_expr_indexes),
        ]
        .concat();

        [input, find_source_indexes(&join.right), expressions].concat()
    }

    fn find_filter_indexes(filter: &FilterPlan) -> Vec<&TableAccessPlan> {
        [
            match &filter.input {
                FilterInputPlan::Source(source) => find_source_indexes(source),
                FilterInputPlan::InnerJoin(join) => find_inner_join_indexes(join),
                FilterInputPlan::LeftOuterJoin(join) => find_left_outer_join_indexes(join),
            },
            find_expr_indexes(&filter.expr),
        ]
        .concat()
    }

    fn find_aggregation_input_indexes(input: &AggregationInputPlan) -> Vec<&TableAccessPlan> {
        match input {
            AggregationInputPlan::Source(source) => find_source_indexes(source),
            AggregationInputPlan::InnerJoin(join) => find_inner_join_indexes(join),
            AggregationInputPlan::LeftOuterJoin(join) => find_left_outer_join_indexes(join),
            AggregationInputPlan::Filter(filter) => find_filter_indexes(filter),
        }
    }

    fn find_offset_indexes(offset: &OffsetPlan) -> Vec<&TableAccessPlan> {
        match &offset.input {
            OffsetInputPlan::Project(project) => find_project_indexes(project),
            OffsetInputPlan::Values(_) | OffsetInputPlan::ValuesOrderBy(_) => Vec::new(),
            OffsetInputPlan::SelectOrderBy(order_by) => find_project_indexes(&order_by.input),
            OffsetInputPlan::Distinct(distinct) => find_distinct_indexes(distinct),
        }
    }

    fn find_project_indexes(project: &ProjectPlan) -> Vec<&TableAccessPlan> {
        match &project.input {
            ProjectInputPlan::Source(source) => find_source_indexes(source),
            ProjectInputPlan::InnerJoin(join) => find_inner_join_indexes(join),
            ProjectInputPlan::LeftOuterJoin(join) => find_left_outer_join_indexes(join),
            ProjectInputPlan::Filter(filter) => find_filter_indexes(filter),
            ProjectInputPlan::Aggregation(aggregation) => {
                find_aggregation_input_indexes(&aggregation.input)
            }
            ProjectInputPlan::Having(having) => find_aggregation_input_indexes(&having.input.input),
        }
    }

    fn find_distinct_indexes(distinct: &DistinctPlan) -> Vec<&TableAccessPlan> {
        match &distinct.input {
            DistinctInputPlan::Project(project) => find_project_indexes(project),
            DistinctInputPlan::SelectOrderBy(order_by) => find_project_indexes(&order_by.input),
        }
    }

    fn find_query_indexes(query: &QueryPlan) -> Vec<&TableAccessPlan> {
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
