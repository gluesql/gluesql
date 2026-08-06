use {
    super::expr::visit_mut_expr,
    crate::plan::{
        AggregateFunctionPlan, AggregationInputPlan, AggregationPlan, DistinctInputPlan,
        DistinctPlan, ExprPlan, FilterInputPlan, FilterPlan, HashJoinInputPlan, HashJoinPlan,
        InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan, JoinConditionPlan,
        LeftOuterJoinInputPlan, LeftOuterJoinPlan, LimitInputPlan, LimitPlan,
        NestedLoopJoinInputPlan, NestedLoopJoinPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan,
        ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan,
        SelectOrderByPlan, SourcePlan, StatementPlan, ValuesOrderByPlan, ValuesPlan,
    },
    std::collections::HashMap,
};

pub fn plan(statement: StatementPlan) -> StatementPlan {
    match statement {
        StatementPlan::Query(mut query) => {
            plan_query(&mut query);
            StatementPlan::Query(query)
        }
        StatementPlan::Insert {
            table_name,
            columns,
            mut source,
        } => {
            plan_query(&mut source);
            StatementPlan::Insert {
                table_name,
                columns,
                source,
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
                plan_query(source);
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
                plan_expr(&mut assignment.value);
            }

            if let Some(selection) = selection.as_mut() {
                plan_expr(selection);
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
                plan_expr(selection);
            }

            StatementPlan::Delete {
                table_name,
                selection,
            }
        }
        _ => statement,
    }
}

fn plan_query(query: &mut QueryPlan) {
    match query {
        QueryPlan::Project(project) => plan_project_query(project, &mut []),
        QueryPlan::Values(values) => plan_values(values),
        QueryPlan::SelectOrderBy(order_by) => plan_select_order_by(order_by),
        QueryPlan::ValuesOrderBy(order_by) => plan_values_order_by(order_by),
        QueryPlan::Distinct(distinct) => plan_distinct(distinct),
        QueryPlan::Offset(offset) => plan_offset(offset),
        QueryPlan::Limit(LimitPlan { input, count }) => {
            match input {
                LimitInputPlan::Project(project) => plan_project_query(project, &mut []),
                LimitInputPlan::Values(values) => plan_values(values),
                LimitInputPlan::SelectOrderBy(order_by) => plan_select_order_by(order_by),
                LimitInputPlan::ValuesOrderBy(order_by) => plan_values_order_by(order_by),
                LimitInputPlan::Distinct(distinct) => plan_distinct(distinct),
                LimitInputPlan::Offset(offset) => plan_offset(offset),
            }

            plan_expr(count);
        }
    }
}

fn plan_offset(OffsetPlan { input, count }: &mut OffsetPlan) {
    match input {
        OffsetInputPlan::Project(project) => plan_project_query(project, &mut []),
        OffsetInputPlan::Values(values) => plan_values(values),
        OffsetInputPlan::SelectOrderBy(order_by) => plan_select_order_by(order_by),
        OffsetInputPlan::ValuesOrderBy(order_by) => plan_values_order_by(order_by),
        OffsetInputPlan::Distinct(distinct) => plan_distinct(distinct),
    }
    plan_expr(count);
}

fn plan_distinct(DistinctPlan { input }: &mut DistinctPlan) {
    match input {
        DistinctInputPlan::Project(project) => plan_project_query(project, &mut []),
        DistinctInputPlan::SelectOrderBy(order_by) => plan_select_order_by(order_by),
    }
}

fn plan_select_order_by(SelectOrderByPlan { input, exprs }: &mut SelectOrderByPlan) {
    plan_project_query(input, exprs);
}

fn plan_values_order_by(ValuesOrderByPlan { input, exprs }: &mut ValuesOrderByPlan) {
    plan_values(input);
    for order_by in exprs {
        plan_expr(&mut order_by.expr);
    }
}

fn plan_project_query(project: &mut ProjectPlan, order_by: &mut [OrderByExprPlan]) {
    plan_project_input(&mut project.input);
    plan_projection(&mut project.projection);
    for order_by in order_by.iter_mut() {
        plan_expr(&mut order_by.expr);
    }
    bind_project(&mut project.input, &mut project.projection, order_by);
}

fn plan_project_input(input: &mut ProjectInputPlan) {
    match input {
        ProjectInputPlan::Source(relation) => plan_source(relation),
        ProjectInputPlan::InnerJoin(join) => plan_inner_join(join),
        ProjectInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join),
        ProjectInputPlan::Filter(filter) => plan_filter(filter),
        ProjectInputPlan::Aggregation(aggregation) => {
            plan_aggregation_input(&mut aggregation.input);
            for group_by in &mut aggregation.group_by {
                plan_expr(group_by);
            }
        }
        ProjectInputPlan::Having(having) => {
            plan_aggregation_input(&mut having.input.input);
            for group_by in &mut having.input.group_by {
                plan_expr(group_by);
            }
            plan_expr(&mut having.expr);
        }
    }
}

fn plan_values(ValuesPlan(exprs_list): &mut ValuesPlan) {
    for exprs in exprs_list {
        for expr in exprs {
            plan_expr(expr);
        }
    }
}

fn plan_filter(FilterPlan { input, expr }: &mut FilterPlan) {
    match input {
        FilterInputPlan::Source(relation) => plan_source(relation),
        FilterInputPlan::InnerJoin(join) => plan_inner_join(join),
        FilterInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join),
    }
    plan_expr(expr);
}

fn plan_aggregation_input(input: &mut AggregationInputPlan) {
    match input {
        AggregationInputPlan::Source(relation) => plan_source(relation),
        AggregationInputPlan::InnerJoin(join) => plan_inner_join(join),
        AggregationInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join),
        AggregationInputPlan::Filter(filter) => plan_filter(filter),
    }
}

fn plan_projection(projection: &mut ProjectionPlan) {
    match projection {
        ProjectionPlan::SelectItems(items) => {
            for item in items {
                if let SelectItemPlan::Expr { expr, .. } = item {
                    plan_expr(expr);
                }
            }
        }
        ProjectionPlan::SchemalessMap => {}
    }
}

fn plan_inner_join(join: &mut InnerJoinPlan) {
    match &mut join.input {
        InnerJoinInputPlan::NestedLoop(join) => plan_nested_loop_join(join),
        InnerJoinInputPlan::Hash(join) => plan_hash_join(join),
        InnerJoinInputPlan::Condition(condition) => plan_join_condition(condition),
    }
}

fn plan_left_outer_join(join: &mut LeftOuterJoinPlan) {
    match &mut join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => plan_nested_loop_join(join),
        LeftOuterJoinInputPlan::Hash(join) => plan_hash_join(join),
        LeftOuterJoinInputPlan::Condition(condition) => plan_join_condition(condition),
    }
}

fn plan_join_condition(condition: &mut JoinConditionPlan) {
    match &mut condition.input {
        JoinConditionInputPlan::NestedLoop(join) => plan_nested_loop_join(join),
        JoinConditionInputPlan::Hash(join) => plan_hash_join(join),
    }
    plan_expr(&mut condition.expr);
}

fn plan_nested_loop_join(join: &mut NestedLoopJoinPlan) {
    match &mut join.input {
        NestedLoopJoinInputPlan::Source(source) => plan_source(source),
        NestedLoopJoinInputPlan::InnerJoin(join) => plan_inner_join(join),
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join),
    }
    plan_source(&mut join.right);
}

fn plan_hash_join(join: &mut HashJoinPlan) {
    match &mut join.input {
        HashJoinInputPlan::Source(source) => plan_source(source),
        HashJoinInputPlan::InnerJoin(join) => plan_inner_join(join),
        HashJoinInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join),
    }
    plan_source(&mut join.right);
    plan_expr(&mut join.input_key);
    plan_expr(&mut join.right_key);

    if let Some(right_filter) = &mut join.right_filter {
        plan_expr(right_filter);
    }
}

fn plan_source(source: &mut SourcePlan) {
    match source {
        SourcePlan::Table(_) | SourcePlan::Dictionary(_) => {}
        SourcePlan::Derived(derived) => plan_query(&mut derived.query),
        SourcePlan::Series(series) => plan_expr(&mut series.size),
    }
}

fn plan_expr(expr: &mut ExprPlan) {
    visit_mut_expr(expr, &mut |expr| match expr {
        ExprPlan::Subquery(subquery)
        | ExprPlan::Exists { subquery, .. }
        | ExprPlan::InSubquery { subquery, .. } => plan_query(subquery),
        _ => {}
    });
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggregateKey {
    func: AggregateFunctionPlan,
    distinct: bool,
}

fn bind_project(
    input: &mut ProjectInputPlan,
    projection: &mut ProjectionPlan,
    order_by: &mut [OrderByExprPlan],
) {
    let mut slots = HashMap::new();
    let mut aggregates = Vec::new();
    let mut bind = |expr: &mut ExprPlan| {
        visit_mut_expr(expr, &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                let key = AggregateKey {
                    func: aggregate.func.clone(),
                    distinct: aggregate.distinct,
                };

                let slot = *slots.entry(key).or_insert_with(|| {
                    let slot = aggregates.len();
                    let mut aggregate = aggregate.as_ref().clone();
                    aggregate.slot = Some(slot);
                    aggregates.push(aggregate);
                    slot
                });

                aggregate.slot = Some(slot);
            }
        });
    };

    if let ProjectionPlan::SelectItems(items) = projection {
        for item in items {
            if let SelectItemPlan::Expr { expr, .. } = item {
                bind(expr);
            }
        }
    }

    if let ProjectInputPlan::Having(having) = input {
        bind(&mut having.expr);
    }

    for order_by in order_by {
        bind(&mut order_by.expr);
    }

    match input {
        ProjectInputPlan::Source(relation) if !aggregates.is_empty() => {
            *input = ProjectInputPlan::Aggregation(AggregationPlan {
                input: AggregationInputPlan::Source(relation.clone()),
                group_by: Vec::new(),
                aggregate_slots: aggregates,
            });
        }
        ProjectInputPlan::InnerJoin(join) if !aggregates.is_empty() => {
            *input = ProjectInputPlan::Aggregation(AggregationPlan {
                input: AggregationInputPlan::InnerJoin(join.clone()),
                group_by: Vec::new(),
                aggregate_slots: aggregates,
            });
        }
        ProjectInputPlan::LeftOuterJoin(join) if !aggregates.is_empty() => {
            *input = ProjectInputPlan::Aggregation(AggregationPlan {
                input: AggregationInputPlan::LeftOuterJoin(join.clone()),
                group_by: Vec::new(),
                aggregate_slots: aggregates,
            });
        }
        ProjectInputPlan::Filter(filter) if !aggregates.is_empty() => {
            *input = ProjectInputPlan::Aggregation(AggregationPlan {
                input: AggregationInputPlan::Filter(filter.clone()),
                group_by: Vec::new(),
                aggregate_slots: aggregates,
            });
        }
        ProjectInputPlan::Source(_)
        | ProjectInputPlan::InnerJoin(_)
        | ProjectInputPlan::LeftOuterJoin(_)
        | ProjectInputPlan::Filter(_) => {}
        ProjectInputPlan::Aggregation(aggregation) => {
            aggregation.aggregate_slots = aggregates;
        }
        ProjectInputPlan::Having(having) => {
            having.input.aggregate_slots = aggregates;
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{plan, plan_query},
        crate::{
            ast::{BinaryOperator, Dictionary, Literal},
            parse_sql::{parse, parse_query},
            plan::{
                AggregateExprPlan, AggregateFunctionPlan, AggregationInputPlan, AggregationPlan,
                AssignmentPlan, CountArgExprPlan, DerivedSourcePlan, DictionarySourcePlan,
                DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan, FilterPlan,
                FunctionExprPlan, HashJoinInputPlan, HashJoinPlan, HavingPlan, InnerJoinInputPlan,
                InnerJoinPlan, JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan,
                LeftOuterJoinPlan, LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan,
                NestedLoopJoinPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan, ProjectInputPlan,
                ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan, SelectOrderByPlan,
                SeriesSourcePlan, SourcePlan, StatementPlan, TableAccessPlan, TableAliasPlan,
                TableSourcePlan, ValuesPlan,
            },
            query_builder::{Build, table},
            translate::{NO_PARAMS, translate, translate_query},
        },
        pretty_assertions::assert_eq,
    };

    fn statement(sql: &str) -> StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);

        StatementPlan::from(translate(&parsed).expect(sql))
    }

    fn parse_and_plan(sql: &str) -> StatementPlan {
        plan(statement(sql))
    }

    fn parse_query_plan(sql: &str) -> QueryPlan {
        let parsed = parse_query(sql).expect(sql);
        translate_query(&parsed, NO_PARAMS)
            .map(QueryPlan::from)
            .expect(sql)
    }

    fn parse_and_plan_query(sql: &str) -> QueryPlan {
        let mut query = parse_query_plan(sql);
        plan_query(&mut query);

        query
    }

    fn count_wildcard(slot: Option<usize>) -> AggregateExprPlan {
        AggregateExprPlan {
            func: AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard),
            distinct: false,
            slot,
        }
    }

    fn count_wildcard_expr(slot: Option<usize>) -> ExprPlan {
        ExprPlan::Aggregate(Box::new(count_wildcard(slot)))
    }

    fn count_distinct_id(slot: Option<usize>) -> AggregateExprPlan {
        AggregateExprPlan {
            func: AggregateFunctionPlan::Count(CountArgExprPlan::Expr(ExprPlan::Identifier(
                "id".to_owned(),
            ))),
            distinct: true,
            slot,
        }
    }

    fn count_distinct_id_expr(slot: Option<usize>) -> ExprPlan {
        ExprPlan::Aggregate(Box::new(count_distinct_id(slot)))
    }

    fn greater_than(left: ExprPlan, right: ExprPlan) -> ExprPlan {
        ExprPlan::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Gt,
            right: Box::new(right),
        }
    }

    fn subquery(query: QueryPlan) -> ExprPlan {
        ExprPlan::Subquery(Box::new(query))
    }

    fn table_source(name: &str) -> SourcePlan {
        SourcePlan::Table(TableSourcePlan {
            name: name.to_owned(),
            alias: None,
            access: TableAccessPlan::FullScan,
        })
    }

    fn table_alias(name: &str) -> TableAliasPlan {
        TableAliasPlan {
            name: name.to_owned(),
            columns: Vec::new(),
        }
    }

    #[test]
    fn binds_same_aggregate_to_same_slot() {
        let actual = parse_and_plan_query(
            "
            SELECT COALESCE(COUNT(*), 0)
            FROM Item
            HAVING COUNT(*) > 0
            ORDER BY COUNT(*)
        ",
        );
        let expected = QueryPlan::SelectOrderBy(SelectOrderByPlan {
            input: ProjectPlan {
                input: ProjectInputPlan::Having(HavingPlan {
                    input: AggregationPlan {
                        input: AggregationInputPlan::Source(table_source("Item")),
                        group_by: Vec::new(),
                        aggregate_slots: vec![count_wildcard(Some(0))],
                    },
                    expr: greater_than(
                        count_wildcard_expr(Some(0)),
                        ExprPlan::Literal(Literal::Number(0.into())),
                    ),
                }),
                projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
                    expr: ExprPlan::Function(Box::new(FunctionExprPlan::Coalesce(vec![
                        count_wildcard_expr(Some(0)),
                        ExprPlan::Literal(Literal::Number(0.into())),
                    ]))),
                    label: "COALESCE(COUNT(*), 0)".to_owned(),
                }]),
            },
            exprs: vec![OrderByExprPlan {
                expr: count_wildcard_expr(Some(0)),
                asc: None,
            }],
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn plans_select_distinct_separately_from_aggregate_distinct() {
        let actual = parse_and_plan_query(
            "
            SELECT DISTINCT COUNT(DISTINCT id)
            FROM Item
            ORDER BY COUNT(DISTINCT id)
        ",
        );
        let expected = QueryPlan::Distinct(DistinctPlan {
            input: DistinctInputPlan::SelectOrderBy(SelectOrderByPlan {
                input: ProjectPlan {
                    input: ProjectInputPlan::Aggregation(AggregationPlan {
                        input: AggregationInputPlan::Source(table_source("Item")),
                        group_by: Vec::new(),
                        aggregate_slots: vec![count_distinct_id(Some(0))],
                    }),
                    projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
                        expr: count_distinct_id_expr(Some(0)),
                        label: "COUNT(DISTINCT id)".to_owned(),
                    }]),
                },
                exprs: vec![OrderByExprPlan {
                    expr: count_distinct_id_expr(Some(0)),
                    asc: None,
                }],
            }),
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn ignores_stale_slot_when_binding_same_aggregate() {
        let aggregation = AggregationPlan {
            input: AggregationInputPlan::Source(table_source("Item")),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let query = |slot: Option<usize>| {
            QueryPlan::Project(ProjectPlan {
                input: ProjectInputPlan::Having(HavingPlan {
                    input: AggregationPlan {
                        aggregate_slots: slot
                            .map(|slot| vec![count_wildcard(Some(slot))])
                            .unwrap_or_default(),
                        ..aggregation.clone()
                    },
                    expr: greater_than(
                        count_wildcard_expr(slot),
                        ExprPlan::Literal(Literal::Number(0.into())),
                    ),
                }),
                projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
                    expr: count_wildcard_expr(slot),
                    label: "COUNT(*)".to_owned(),
                }]),
            })
        };
        let mut actual = query(Some(99));

        plan_query(&mut actual);

        let expected = query(Some(0));
        assert_eq!(actual, expected);
    }

    #[test]
    fn binds_subqueries_per_select() {
        let query = parse_and_plan_query(
            "
            SELECT COUNT(*)
            FROM (SELECT COUNT(*) FROM Item) AS sub
        ",
        );
        let actual = query.project().map(|project| &project.input);
        let expected = ProjectInputPlan::Aggregation(AggregationPlan {
            input: AggregationInputPlan::Source(SourcePlan::Derived(DerivedSourcePlan {
                query: Box::new(parse_and_plan_query("SELECT COUNT(*) FROM Item")),
                alias: table_alias("sub"),
            })),
            group_by: Vec::new(),
            aggregate_slots: vec![count_wildcard(Some(0))],
        });
        assert_eq!(actual, Some(&expected));
    }

    #[test]
    fn binds_insert_and_create_table_source_queries() {
        let actual = parse_and_plan("INSERT INTO Target SELECT COUNT(*) FROM Source");
        let expected = StatementPlan::Insert {
            table_name: "Target".to_owned(),
            columns: Vec::new(),
            source: parse_and_plan_query("SELECT COUNT(*) FROM Source"),
        };
        assert_eq!(actual, expected);

        let actual = parse_and_plan("CREATE TABLE Target AS SELECT COUNT(*) FROM Source");
        let expected = StatementPlan::CreateTable {
            if_not_exists: false,
            name: "Target".to_owned(),
            columns: None,
            source: Some(Box::new(parse_and_plan_query(
                "SELECT COUNT(*) FROM Source",
            ))),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn binds_update_and_delete_expr_subqueries() {
        let actual = parse_and_plan("UPDATE Target SET count = (SELECT COUNT(*) FROM Source)");
        let expected = StatementPlan::Update {
            table_name: "Target".to_owned(),
            assignments: vec![AssignmentPlan {
                id: "count".to_owned(),
                value: ExprPlan::Subquery(Box::new(parse_and_plan_query(
                    "SELECT COUNT(*) FROM Source",
                ))),
            }],
            selection: None,
        };
        assert_eq!(actual, expected);

        let actual =
            parse_and_plan("UPDATE Target SET count = 1 WHERE id = (SELECT COUNT(*) FROM Source)");
        let expected = StatementPlan::Update {
            table_name: "Target".to_owned(),
            assignments: vec![AssignmentPlan {
                id: "count".to_owned(),
                value: ExprPlan::Literal(Literal::Number(1.into())),
            }],
            selection: Some(ExprPlan::BinaryOp {
                left: Box::new(ExprPlan::Identifier("id".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(ExprPlan::Subquery(Box::new(parse_and_plan_query(
                    "SELECT COUNT(*) FROM Source",
                )))),
            }),
        };
        assert_eq!(actual, expected);

        let actual = parse_and_plan("DELETE FROM Target WHERE id = (SELECT COUNT(*) FROM Source)");
        let expected = StatementPlan::Delete {
            table_name: "Target".to_owned(),
            selection: Some(ExprPlan::BinaryOp {
                left: Box::new(ExprPlan::Identifier("id".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(ExprPlan::Subquery(Box::new(parse_and_plan_query(
                    "SELECT COUNT(*) FROM Source",
                )))),
            }),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn keeps_create_table_without_source_unplanned() {
        let actual = parse_and_plan("CREATE TABLE Target (id INTEGER)");
        let expected = table("Target")
            .create_table()
            .add_column("id INTEGER")
            .build()
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn keeps_non_query_statements_unchanged() {
        let actual = parse_and_plan("SHOW COLUMNS FROM Target");
        let expected = table("Target").show_columns().build().unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn plans_values_limit_and_offset_subqueries() {
        let project = parse_query_plan("SELECT id FROM Item")
            .project()
            .expect("expected project")
            .clone();
        let query = |count_query: QueryPlan| {
            QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::Project(project.clone()),
                    count: subquery(count_query.clone()),
                }),
                count: subquery(count_query),
            })
        };
        let mut actual = query(parse_query_plan("SELECT COUNT(*) FROM Item"));

        plan_query(&mut actual);

        let expected = query(parse_and_plan_query("SELECT COUNT(*) FROM Item"));
        assert_eq!(actual, expected);

        let actual = parse_and_plan_query("VALUES ((SELECT COUNT(*) FROM Item))");
        let expected = QueryPlan::Values(ValuesPlan(vec![vec![subquery(parse_and_plan_query(
            "SELECT COUNT(*) FROM Item",
        ))]]));
        assert_eq!(actual, expected);
    }

    #[test]
    fn plans_selection_group_by_and_in_subquery_exprs() {
        let query = parse_and_plan_query(
            "
            SELECT id
            FROM Item
            WHERE EXISTS (SELECT COUNT(*) FROM Source)
            GROUP BY id IN (SELECT COUNT(*) FROM Source)
        ",
        );
        let actual = query.project().map(|project| &project.input);
        let expected = ProjectInputPlan::Aggregation(AggregationPlan {
            input: AggregationInputPlan::Filter(FilterPlan {
                input: FilterInputPlan::Source(table_source("Item")),
                expr: ExprPlan::Exists {
                    subquery: Box::new(parse_and_plan_query("SELECT COUNT(*) FROM Source")),
                    negated: false,
                },
            }),
            group_by: vec![ExprPlan::InSubquery {
                expr: Box::new(ExprPlan::Identifier("id".to_owned())),
                subquery: Box::new(parse_and_plan_query("SELECT COUNT(*) FROM Source")),
                negated: false,
            }],
            aggregate_slots: Vec::new(),
        });

        assert_eq!(actual, Some(&expected));
    }

    #[test]
    fn keeps_select_without_aggregates_unplanned() {
        let actual = parse_and_plan("SELECT * FROM Item");
        let expected = table("Item").select().build().unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn preserves_explicit_aggregation_and_having_stages_without_slots() {
        let actual = parse_and_plan("SELECT category FROM Item GROUP BY category");
        let expected = table("Item")
            .select()
            .group_by("category")
            .project("category")
            .build()
            .unwrap();
        assert_eq!(actual, expected);

        let actual = parse_and_plan("SELECT 1 FROM Item HAVING TRUE");
        let expected = table("Item")
            .select()
            .having("TRUE")
            .project("1")
            .build()
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn promotes_aggregate_only_projection_and_order_by() {
        let query = parse_and_plan_query("SELECT COUNT(*) FROM Item");
        let actual = query.project().map(|project| &project.input);
        let expected = ProjectInputPlan::Aggregation(AggregationPlan {
            input: AggregationInputPlan::Source(table_source("Item")),
            group_by: Vec::new(),
            aggregate_slots: vec![count_wildcard(Some(0))],
        });
        assert_eq!(actual, Some(&expected));

        let query = parse_and_plan_query("SELECT id FROM Item ORDER BY COUNT(*)");
        let actual = query.project().map(|project| &project.input);
        let expected = ProjectInputPlan::Aggregation(AggregationPlan {
            input: AggregationInputPlan::Source(table_source("Item")),
            group_by: Vec::new(),
            aggregate_slots: vec![count_wildcard(Some(0))],
        });
        assert_eq!(actual, Some(&expected));
    }

    #[test]
    fn binds_aggregate_used_only_by_having() {
        let query = parse_and_plan_query("SELECT 1 FROM Item HAVING COUNT(*) > 0");
        let actual = query.project().map(|project| &project.input);
        let expected = ProjectInputPlan::Having(HavingPlan {
            input: AggregationPlan {
                input: AggregationInputPlan::Source(table_source("Item")),
                group_by: Vec::new(),
                aggregate_slots: vec![count_wildcard(Some(0))],
            },
            expr: greater_than(
                count_wildcard_expr(Some(0)),
                ExprPlan::Literal(Literal::Number(0.into())),
            ),
        });

        assert_eq!(actual, Some(&expected));
    }

    #[test]
    fn keeps_schemaless_projection_unplanned() {
        let mut actual = QueryPlan::Project(ProjectPlan {
            projection: ProjectionPlan::SchemalessMap,
            input: ProjectInputPlan::Source(SourcePlan::Dictionary(DictionarySourcePlan {
                dictionary: Dictionary::GlueTables,
                alias: table_alias("GLUE_TABLES"),
            })),
        });
        let expected = actual.clone();

        plan_query(&mut actual);

        assert_eq!(actual, expected);
    }

    #[test]
    fn plans_source_join_and_hash_executor_exprs() {
        fn query(nested_query: &QueryPlan) -> QueryPlan {
            let nested_subquery = || subquery(nested_query.clone());
            let first_join = InnerJoinPlan {
                input: InnerJoinInputPlan::Condition(JoinConditionPlan {
                    input: JoinConditionInputPlan::Hash(HashJoinPlan {
                        input: HashJoinInputPlan::Source(SourcePlan::Derived(DerivedSourcePlan {
                            query: Box::new(nested_query.clone()),
                            alias: table_alias("derived"),
                        })),
                        right: SourcePlan::Series(SeriesSourcePlan {
                            alias: table_alias("series"),
                            size: nested_subquery(),
                        }),
                        input_key: nested_subquery(),
                        right_key: nested_subquery(),
                        right_filter: Some(nested_subquery()),
                    }),
                    expr: nested_subquery(),
                }),
            };
            let second_join = LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Hash(HashJoinPlan {
                    input: HashJoinInputPlan::InnerJoin(Box::new(first_join)),
                    right: table_source("Target"),
                    input_key: nested_subquery(),
                    right_key: nested_subquery(),
                    right_filter: None,
                }),
            };
            let third_join = InnerJoinPlan {
                input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                    input: NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(second_join)),
                    right: SourcePlan::Dictionary(DictionarySourcePlan {
                        dictionary: Dictionary::GlueIndexes,
                        alias: table_alias("GLUE_INDEXES"),
                    }),
                }),
            };

            QueryPlan::SelectOrderBy(SelectOrderByPlan {
                input: ProjectPlan {
                    input: ProjectInputPlan::InnerJoin(Box::new(third_join)),
                    projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
                },
                exprs: vec![OrderByExprPlan {
                    expr: ExprPlan::Literal(Literal::Number(1.into())),
                    asc: None,
                }],
            })
        }

        let mut actual = query(&parse_query_plan("SELECT COUNT(*) FROM Item"));
        plan_query(&mut actual);
        let expected = query(&parse_and_plan_query("SELECT COUNT(*) FROM Item"));

        assert_eq!(actual, expected);
    }
}
