use {
    super::{PlannerError, expr::visit_mut_expr},
    crate::{
        plan::{
            AggregationInputPlan, DerivedSourcePlan, DistinctInputPlan, DistinctPlan, ExprPlan,
            FilterInputPlan, FilterPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan,
            InnerJoinPlan, JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan,
            LeftOuterJoinPlan, LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan,
            NestedLoopJoinPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan, ProjectInputPlan,
            ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan, SelectOrderByPlan, SourcePlan,
            StatementPlan, TableAliasPlan, TableSourcePlan, ValuesOrderByPlan, ValuesPlan,
        },
        result::Result,
    },
    std::collections::{HashMap, HashSet},
};

/// Resolve CTEs before any storage names are collected.
pub fn plan(statement: StatementPlan) -> Result<StatementPlan> {
    let mut planner = CtePlanner::default();
    let statement = planner.statement(statement);
    match planner.error {
        Some(error) => Err(error.into()),
        None => Ok(statement),
    }
}

#[derive(Default)]
struct CtePlanner {
    scope: HashMap<String, QueryPlan>,
    error: Option<PlannerError>,
}

/// Execution policy for a resolved definition. Name lookup is independent of inlining.
fn inline_reference(table: &TableSourcePlan, definition: QueryPlan) -> SourcePlan {
    SourcePlan::Derived(DerivedSourcePlan {
        query: Box::new(definition),
        alias: table.alias.clone().unwrap_or_else(|| TableAliasPlan {
            name: table.name.clone(),
            columns: Vec::new(),
        }),
    })
}

impl CtePlanner {
    fn resolve_name(&self, name: &str) -> Option<QueryPlan> {
        self.scope.get(name).cloned()
    }
    fn statement(&mut self, statement: StatementPlan) -> StatementPlan {
        match statement {
            StatementPlan::Query(mut query) => {
                self.plan_query(&mut query);
                StatementPlan::Query(query)
            }
            StatementPlan::Insert {
                table_name,
                columns,
                mut source,
                table_columns,
            } => {
                self.plan_query(&mut source);
                StatementPlan::Insert {
                    table_name,
                    columns,
                    source,
                    table_columns,
                }
            }
            StatementPlan::CreateTable {
                if_not_exists,
                name,
                columns,
                mut source,
                engine,
                foreign_keys,
                comment,
            } => {
                if let Some(source) = source.as_mut() {
                    self.plan_query(source);
                }

                StatementPlan::CreateTable {
                    if_not_exists,
                    name,
                    columns,
                    source,
                    engine,
                    foreign_keys,
                    comment,
                }
            }
            StatementPlan::Update {
                table_name,
                mut assignments,
                mut selection,
            } => {
                for assignment in &mut assignments {
                    self.plan_expr(&mut assignment.value);
                }

                if let Some(selection) = selection.as_mut() {
                    self.plan_expr(selection);
                }

                StatementPlan::Update {
                    table_name,
                    assignments,
                    selection,
                }
            }
            StatementPlan::Delete {
                table_name,
                mut selection,
            } => {
                if let Some(selection) = selection.as_mut() {
                    self.plan_expr(selection);
                }

                StatementPlan::Delete {
                    table_name,
                    selection,
                }
            }
            _ => statement,
        }
    }

    fn plan_query(&mut self, query: &mut QueryPlan) {
        if self.error.is_some() {
            return;
        }
        match query {
            QueryPlan::UnplannedWith(ast_query) => {
                let mut ast_query = *ast_query.clone();
                let definitions = std::mem::take(&mut ast_query.with);
                let outer = self.scope.clone();
                let mut local_names = HashSet::new();
                for cte in definitions {
                    if !local_names.insert(cte.alias.name.clone()) {
                        self.error = Some(PlannerError::DuplicateCteName(cte.alias.name.clone()));
                        self.scope = outer;
                        return;
                    }
                    let mut definition = QueryPlan::from(cte.query);
                    self.plan_query(&mut definition);
                    if self.error.is_some() {
                        self.scope = outer;
                        return;
                    }
                    self.scope.insert(cte.alias.name.clone(), definition);
                }
                *query = ast_query.into();
                self.plan_query(query);
                self.scope = outer;
            }
            QueryPlan::Project(project) => self.plan_project_query(project, &mut []),
            QueryPlan::Values(values) => self.plan_values(values),
            QueryPlan::SelectOrderBy(order_by) => self.plan_select_order_by(order_by),
            QueryPlan::ValuesOrderBy(order_by) => self.plan_values_order_by(order_by),
            QueryPlan::Distinct(distinct) => self.plan_distinct(distinct),
            QueryPlan::Offset(offset) => self.plan_offset(offset),
            QueryPlan::Limit(LimitPlan { input, count }) => {
                match input {
                    LimitInputPlan::Project(project) => self.plan_project_query(project, &mut []),
                    LimitInputPlan::Values(values) => self.plan_values(values),
                    LimitInputPlan::SelectOrderBy(order_by) => self.plan_select_order_by(order_by),
                    LimitInputPlan::ValuesOrderBy(order_by) => self.plan_values_order_by(order_by),
                    LimitInputPlan::Distinct(distinct) => self.plan_distinct(distinct),
                    LimitInputPlan::Offset(offset) => self.plan_offset(offset),
                }

                self.plan_expr(count);
            }
        }
    }

    fn plan_offset(&mut self, OffsetPlan { input, count }: &mut OffsetPlan) {
        match input {
            OffsetInputPlan::Project(project) => self.plan_project_query(project, &mut []),
            OffsetInputPlan::Values(values) => self.plan_values(values),
            OffsetInputPlan::SelectOrderBy(order_by) => self.plan_select_order_by(order_by),
            OffsetInputPlan::ValuesOrderBy(order_by) => self.plan_values_order_by(order_by),
            OffsetInputPlan::Distinct(distinct) => self.plan_distinct(distinct),
        }
        self.plan_expr(count);
    }

    fn plan_distinct(&mut self, DistinctPlan { input }: &mut DistinctPlan) {
        match input {
            DistinctInputPlan::Project(project) => self.plan_project_query(project, &mut []),
            DistinctInputPlan::SelectOrderBy(order_by) => self.plan_select_order_by(order_by),
        }
    }

    fn plan_select_order_by(&mut self, SelectOrderByPlan { input, exprs }: &mut SelectOrderByPlan) {
        self.plan_project_query(input, exprs);
    }

    fn plan_values_order_by(&mut self, ValuesOrderByPlan { input, exprs }: &mut ValuesOrderByPlan) {
        self.plan_values(input);
        for order_by in exprs {
            self.plan_expr(&mut order_by.expr);
        }
    }

    fn plan_project_query(&mut self, project: &mut ProjectPlan, order_by: &mut [OrderByExprPlan]) {
        self.plan_project_input(&mut project.input);
        self.plan_projection(&mut project.projection);
        for order_by in order_by.iter_mut() {
            self.plan_expr(&mut order_by.expr);
        }
    }

    fn plan_project_input(&mut self, input: &mut ProjectInputPlan) {
        match input {
            ProjectInputPlan::Source(relation) => self.plan_source(relation),
            ProjectInputPlan::InnerJoin(join) => self.plan_inner_join(join),
            ProjectInputPlan::LeftOuterJoin(join) => self.plan_left_outer_join(join),
            ProjectInputPlan::Filter(filter) => self.plan_filter(filter),
            ProjectInputPlan::Aggregation(aggregation) => {
                self.plan_aggregation_input(&mut aggregation.input);
                for group_by in &mut aggregation.group_by {
                    self.plan_expr(group_by);
                }
            }
            ProjectInputPlan::Having(having) => {
                self.plan_aggregation_input(&mut having.input.input);
                for group_by in &mut having.input.group_by {
                    self.plan_expr(group_by);
                }
                self.plan_expr(&mut having.expr);
            }
        }
    }

    fn plan_values(&mut self, ValuesPlan(exprs_list): &mut ValuesPlan) {
        for exprs in exprs_list {
            for expr in exprs {
                self.plan_expr(expr);
            }
        }
    }

    fn plan_filter(&mut self, FilterPlan { input, expr }: &mut FilterPlan) {
        match input {
            FilterInputPlan::Source(relation) => self.plan_source(relation),
            FilterInputPlan::InnerJoin(join) => self.plan_inner_join(join),
            FilterInputPlan::LeftOuterJoin(join) => self.plan_left_outer_join(join),
        }
        self.plan_expr(expr);
    }

    fn plan_aggregation_input(&mut self, input: &mut AggregationInputPlan) {
        match input {
            AggregationInputPlan::Source(relation) => self.plan_source(relation),
            AggregationInputPlan::InnerJoin(join) => self.plan_inner_join(join),
            AggregationInputPlan::LeftOuterJoin(join) => self.plan_left_outer_join(join),
            AggregationInputPlan::Filter(filter) => self.plan_filter(filter),
        }
    }

    fn plan_projection(&mut self, projection: &mut ProjectionPlan) {
        match projection {
            ProjectionPlan::SelectItems(items) => {
                for item in items {
                    if let SelectItemPlan::Expr { expr, .. } = item {
                        self.plan_expr(expr);
                    }
                }
            }
            ProjectionPlan::SchemalessMap => {}
        }
    }

    fn plan_inner_join(&mut self, join: &mut InnerJoinPlan) {
        match &mut join.input {
            InnerJoinInputPlan::NestedLoop(join) => self.plan_nested_loop_join(join),
            InnerJoinInputPlan::Hash(join) => self.plan_hash_join(join),
            InnerJoinInputPlan::Condition(condition) => self.plan_join_condition(condition),
        }
    }

    fn plan_left_outer_join(&mut self, join: &mut LeftOuterJoinPlan) {
        match &mut join.input {
            LeftOuterJoinInputPlan::NestedLoop(join) => self.plan_nested_loop_join(join),
            LeftOuterJoinInputPlan::Hash(join) => self.plan_hash_join(join),
            LeftOuterJoinInputPlan::Condition(condition) => self.plan_join_condition(condition),
        }
    }

    fn plan_join_condition(&mut self, condition: &mut JoinConditionPlan) {
        match &mut condition.input {
            JoinConditionInputPlan::NestedLoop(join) => self.plan_nested_loop_join(join),
            JoinConditionInputPlan::Hash(join) => self.plan_hash_join(join),
        }
        self.plan_expr(&mut condition.expr);
    }

    fn plan_nested_loop_join(&mut self, join: &mut NestedLoopJoinPlan) {
        match &mut join.input {
            NestedLoopJoinInputPlan::Source(source) => self.plan_source(source),
            NestedLoopJoinInputPlan::InnerJoin(join) => self.plan_inner_join(join),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => self.plan_left_outer_join(join),
        }
        self.plan_source(&mut join.right);
    }

    fn plan_hash_join(&mut self, join: &mut HashJoinPlan) {
        match &mut join.input {
            HashJoinInputPlan::Source(source) => self.plan_source(source),
            HashJoinInputPlan::InnerJoin(join) => self.plan_inner_join(join),
            HashJoinInputPlan::LeftOuterJoin(join) => self.plan_left_outer_join(join),
        }
        self.plan_source(&mut join.right);
        self.plan_expr(&mut join.input_key);
        self.plan_expr(&mut join.right_key);

        if let Some(right_filter) = &mut join.right_filter {
            self.plan_expr(right_filter);
        }
    }

    fn plan_source(&mut self, source: &mut SourcePlan) {
        match source {
            SourcePlan::Table(table) => {
                if let Some(definition) = self.resolve_name(&table.name) {
                    *source = inline_reference(table, definition);
                }
            }
            SourcePlan::Dictionary(_) => {}
            SourcePlan::Derived(derived) => self.plan_query(&mut derived.query),
            SourcePlan::Series(series) => self.plan_expr(&mut series.size),
        }
    }

    fn plan_expr(&mut self, expr: &mut ExprPlan) {
        visit_mut_expr(expr, &mut |expr| match expr {
            ExprPlan::Subquery(subquery)
            | ExprPlan::Exists { subquery, .. }
            | ExprPlan::InSubquery { subquery, .. } => self.plan_query(subquery),
            _ => {}
        });
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            data::{Key, Schema, Value},
            mock::MockStorage,
            parse_sql::parse,
            plan::{ExprPlan, StatementPlan},
            planner::{
                PlannerError, fetch_schema_map, plan_aggregate, plan_hash_join, plan_index,
                plan_primary_key, plan_schemaless,
            },
            result::{Error, Result},
            store::{RowIter, Store},
            translate::translate,
        },
        std::{cell::RefCell, collections::HashMap},
    };

    #[derive(Default)]
    struct RecordingStorage {
        storage: MockStorage,
        fetched: RefCell<Vec<String>>,
    }

    impl Store for RecordingStorage {
        fn fetch_schema(&self, name: &str) -> Result<Option<Schema>> {
            self.fetched.borrow_mut().push(name.to_owned());
            self.storage.fetch_schema(name)
        }

        fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
            self.storage.fetch_all_schemas()
        }

        fn fetch_data(&self, name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
            self.storage.fetch_data(name, key)
        }

        fn scan_data<'a>(&'a self, name: &str) -> Result<RowIter<'a>> {
            self.storage.scan_data(name)
        }
    }

    fn statement(sql: &str) -> StatementPlan {
        translate(&parse(sql).unwrap().remove(0)).unwrap().into()
    }

    #[test]
    fn cte_names_never_reach_storage_schema_lookup() {
        for (sql, expected) in [
            (
                "WITH a AS (SELECT * FROM physical), b AS (SELECT * FROM a) SELECT * FROM b AS x JOIN b AS y",
                vec!["physical", "physical"],
            ),
            (
                "WITH a AS (SELECT * FROM physical) SELECT * FROM (SELECT * FROM a) AS x",
                vec!["physical"],
            ),
            (
                "WITH a AS (SELECT * FROM b), b AS (SELECT * FROM a) SELECT * FROM b",
                vec!["b"],
            ),
            ("WITH a AS (SELECT * FROM a) SELECT * FROM a", vec!["a"]),
            (
                "WITH a AS (SELECT 1 AS n) SELECT * FROM (WITH a AS (SELECT * FROM a) SELECT * FROM a) AS x",
                vec![],
            ),
        ] {
            let storage = RecordingStorage::default();
            let resolved = plan(statement(sql)).unwrap();
            fetch_schema_map(&storage, &resolved).unwrap();
            assert_eq!(*storage.fetched.borrow(), expected, "{sql}");
        }
    }

    #[test]
    fn duplicate_names_in_nested_definitions_propagate() {
        for sql in [
            "WITH a AS (WITH d AS (SELECT 1), d AS (SELECT 2) SELECT * FROM d) SELECT * FROM a",
            "WITH a AS (SELECT 1) SELECT (WITH d AS (SELECT 1), d AS (SELECT 2) SELECT * FROM d), (SELECT 1)",
        ] {
            assert_eq!(
                plan(statement(sql)),
                Err(Error::Planner(PlannerError::DuplicateCteName("d".into())))
            );
        }
    }

    #[test]
    fn optimization_passes_do_not_make_unplanned_with_executable() {
        let original = statement("WITH t AS (SELECT 1) SELECT * FROM t");
        let schema_map = HashMap::<String, Schema>::new();
        let optimized = plan_primary_key(&schema_map, original.clone());
        let optimized = plan_index(&schema_map, optimized);
        let optimized = plan_hash_join(&schema_map, optimized);
        let optimized = plan_aggregate(optimized);
        assert_eq!(optimized, original);
        let StatementPlan::Query(query) = optimized else {
            panic!("expected query");
        };
        assert!(query.project().is_none());
        assert!(!crate::planner::expr::evaluable::check_expr(
            None,
            &ExprPlan::Subquery(Box::new(query))
        ));

        let storage = crate::mock::run("CREATE TABLE Schemaless");
        let schema = storage.fetch_schema("Schemaless").unwrap().unwrap();
        let schema_map = HashMap::from([("Schemaless".into(), schema)]);
        assert_eq!(
            plan_schemaless(&schema_map, original),
            Err(Error::Planner(PlannerError::UnplannedWith))
        );
    }
}
