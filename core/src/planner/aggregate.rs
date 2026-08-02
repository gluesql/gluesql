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
        super::plan,
        crate::{
            ast::{Dictionary, Literal},
            data::Value,
            parse_sql::parse,
            plan::{
                AggregationInputPlan, AggregationPlan, DerivedSourcePlan, DictionarySourcePlan,
                DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan, FilterPlan,
                HashJoinInputPlan, HashJoinPlan, HavingPlan, InnerJoinInputPlan, InnerJoinPlan,
                JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan,
                LeftOuterJoinPlan, LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan,
                NestedLoopJoinPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan, ProjectInputPlan,
                ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan, SelectOrderByPlan,
                SeriesSourcePlan, SourcePlan, StatementPlan, TableAccessPlan, TableAliasPlan,
                TableSourcePlan,
            },
            planner::expr::{try_visit_expr, visit_mut_expr},
            translate::translate,
        },
    };

    fn parse_and_plan(sql: &str) -> StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        let translated = StatementPlan::from(translate(&parsed).expect(sql));

        plan(translated)
    }

    fn parse_query(sql: &str) -> QueryPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        let StatementPlan::Query(query) = StatementPlan::from(translate(&parsed).expect(sql))
        else {
            panic!("expected query");
        };

        query
    }

    fn project(statement: &StatementPlan) -> &ProjectPlan {
        let StatementPlan::Query(query) = statement else {
            panic!("expected query");
        };
        query.project().expect("expected project")
    }

    fn source_query(query: &QueryPlan) -> Option<&SourcePlan> {
        query.project().map(|project| project.input.base_source())
    }

    fn inner_join_query(query: &QueryPlan) -> Option<&InnerJoinPlan> {
        query.project().and_then(|project| match &project.input {
            ProjectInputPlan::InnerJoin(join) => Some(join.as_ref()),
            ProjectInputPlan::Filter(filter) => match &filter.input {
                FilterInputPlan::InnerJoin(join) => Some(join.as_ref()),
                FilterInputPlan::Source(_) | FilterInputPlan::LeftOuterJoin(_) => None,
            },
            ProjectInputPlan::Aggregation(aggregation) => match &aggregation.input {
                AggregationInputPlan::InnerJoin(join) => Some(join.as_ref()),
                AggregationInputPlan::Filter(filter) => match &filter.input {
                    FilterInputPlan::InnerJoin(join) => Some(join.as_ref()),
                    FilterInputPlan::Source(_) | FilterInputPlan::LeftOuterJoin(_) => None,
                },
                AggregationInputPlan::Source(_) | AggregationInputPlan::LeftOuterJoin(_) => None,
            },
            ProjectInputPlan::Having(having) => match &having.input.input {
                AggregationInputPlan::InnerJoin(join) => Some(join.as_ref()),
                AggregationInputPlan::Filter(filter) => match &filter.input {
                    FilterInputPlan::InnerJoin(join) => Some(join.as_ref()),
                    FilterInputPlan::Source(_) | FilterInputPlan::LeftOuterJoin(_) => None,
                },
                AggregationInputPlan::Source(_) | AggregationInputPlan::LeftOuterJoin(_) => None,
            },
            ProjectInputPlan::Source(_) | ProjectInputPlan::LeftOuterJoin(_) => None,
        })
    }

    fn filter_query(query: &QueryPlan) -> Option<&FilterPlan> {
        query.project().and_then(|project| match &project.input {
            ProjectInputPlan::Filter(filter) => Some(filter),
            ProjectInputPlan::Aggregation(aggregation) => match &aggregation.input {
                AggregationInputPlan::Filter(filter) => Some(filter),
                AggregationInputPlan::Source(_)
                | AggregationInputPlan::InnerJoin(_)
                | AggregationInputPlan::LeftOuterJoin(_) => None,
            },
            ProjectInputPlan::Having(having) => match &having.input.input {
                AggregationInputPlan::Filter(filter) => Some(filter),
                AggregationInputPlan::Source(_)
                | AggregationInputPlan::InnerJoin(_)
                | AggregationInputPlan::LeftOuterJoin(_) => None,
            },
            ProjectInputPlan::Source(_)
            | ProjectInputPlan::InnerJoin(_)
            | ProjectInputPlan::LeftOuterJoin(_) => None,
        })
    }

    fn aggregation_query(query: &QueryPlan) -> Option<&AggregationPlan> {
        query.project().and_then(|project| match &project.input {
            ProjectInputPlan::Aggregation(aggregation) => Some(aggregation),
            ProjectInputPlan::Having(having) => Some(&having.input),
            ProjectInputPlan::Source(_)
            | ProjectInputPlan::InnerJoin(_)
            | ProjectInputPlan::LeftOuterJoin(_)
            | ProjectInputPlan::Filter(_) => None,
        })
    }

    fn having_query(query: &QueryPlan) -> Option<&HavingPlan> {
        query.project().and_then(|project| match &project.input {
            ProjectInputPlan::Having(having) => Some(having),
            ProjectInputPlan::Source(_)
            | ProjectInputPlan::InnerJoin(_)
            | ProjectInputPlan::LeftOuterJoin(_)
            | ProjectInputPlan::Filter(_)
            | ProjectInputPlan::Aggregation(_) => None,
        })
    }

    fn assert_planned_query(query: &QueryPlan) {
        assert_eq!(
            aggregation_query(query)
                .expect("expected aggregation")
                .aggregate_slots
                .len(),
            1
        );
    }

    fn assert_unplanned_query(query: &QueryPlan) {
        let project = query.project().expect("expected project");
        assert!(matches!(project.input, ProjectInputPlan::Source(_)));
    }

    fn count_query() -> QueryPlan {
        parse_query("SELECT COUNT(*) FROM Item")
    }

    fn subquery_expr() -> ExprPlan {
        ExprPlan::Subquery(Box::new(count_query()))
    }

    fn alias(name: &str) -> TableAliasPlan {
        TableAliasPlan {
            name: name.to_owned(),
            columns: Vec::new(),
        }
    }

    #[test]
    fn binds_same_aggregate_to_same_slot() {
        let statement = parse_and_plan(
            "
            SELECT COALESCE(COUNT(*), 0)
            FROM Item
            HAVING COUNT(*) > 0
            ORDER BY COUNT(*)
        ",
        );
        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        let aggregation = aggregation_query(query).expect("expected aggregation");
        let having = having_query(query).expect("expected having");
        let slots = &aggregation.aggregate_slots;
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].slot, Some(0));

        let ProjectionPlan::SelectItems(items) = &project(&statement).projection else {
            panic!("expected select items");
        };
        let SelectItemPlan::Expr { expr, .. } = &items[0] else {
            panic!("expected expression");
        };

        let mut found_slots = Vec::new();
        try_visit_expr(expr, &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                found_slots.push(aggregate.slot);
            }

            Ok(())
        })
        .expect("projection traversal");
        try_visit_expr(&having.expr, &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                found_slots.push(aggregate.slot);
            }

            Ok(())
        })
        .expect("having traversal");

        assert!(matches!(
            query,
            QueryPlan::SelectOrderBy(SelectOrderByPlan { exprs, .. }) if {
                try_visit_expr(&exprs[0].expr, &mut |expr| {
                    if let ExprPlan::Aggregate(aggregate) = expr {
                        found_slots.push(aggregate.slot);
                    }
                    Ok(())
                })
                .expect("order by traversal");
                true
            }
        ));

        assert_eq!(found_slots, vec![Some(0), Some(0), Some(0)]);
    }

    #[test]
    fn plans_select_distinct_separately_from_aggregate_distinct() {
        let statement = parse_and_plan(
            "
            SELECT DISTINCT COUNT(DISTINCT id)
            FROM Item
            ORDER BY COUNT(DISTINCT id)
        ",
        );
        let StatementPlan::Query(QueryPlan::Distinct(DistinctPlan {
            input: DistinctInputPlan::SelectOrderBy(order_by),
        })) = &statement
        else {
            panic!("expected distinct over select order by");
        };
        let ProjectInputPlan::Aggregation(aggregation) = &order_by.input.input else {
            panic!("expected aggregation");
        };
        let slots = &aggregation.aggregate_slots;

        assert_eq!(slots.len(), 1);
        assert!(slots[0].distinct);
        assert_eq!(slots[0].slot, Some(0));

        let mut order_by_slot = None;
        try_visit_expr(&order_by.exprs[0].expr, &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                order_by_slot = aggregate.slot;
            }
            Ok(())
        })
        .expect("order by traversal");
        assert_eq!(order_by_slot, Some(0));
    }

    #[test]
    fn ignores_stale_slot_when_binding_same_aggregate() {
        let mut query = parse_query("SELECT COUNT(*) FROM Item HAVING COUNT(*) > 0");
        let mut project = query.project().expect("expected project").clone();
        let ProjectionPlan::SelectItems(items) = &mut project.projection else {
            panic!("expected select items");
        };
        let SelectItemPlan::Expr { expr, .. } = &mut items[0] else {
            panic!("expected expression");
        };

        visit_mut_expr(expr, &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                aggregate.slot = Some(99);
            }
        });
        query = QueryPlan::Project(project);

        let StatementPlan::Query(query) = plan(StatementPlan::Query(query)) else {
            panic!("expected query");
        };
        let aggregation = aggregation_query(&query).expect("expected aggregation");
        let having = having_query(&query).expect("expected having");
        let slots = &aggregation.aggregate_slots;

        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].slot, Some(0));

        let ProjectionPlan::SelectItems(items) =
            &query.project().expect("expected project").projection
        else {
            panic!("expected select items");
        };
        let SelectItemPlan::Expr { expr, .. } = &items[0] else {
            panic!("expected expression");
        };

        let mut found_slots = Vec::new();
        try_visit_expr(expr, &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                found_slots.push(aggregate.slot);
            }
            Ok(())
        })
        .expect("projection traversal");
        try_visit_expr(&having.expr, &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                found_slots.push(aggregate.slot);
            }
            Ok(())
        })
        .expect("having traversal");

        assert_eq!(found_slots, vec![Some(0), Some(0)]);
    }

    #[test]
    fn binds_subqueries_per_select() {
        let statement = parse_and_plan(
            "
            SELECT COUNT(*)
            FROM (SELECT COUNT(*) FROM Item) AS sub
        ",
        );
        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        let relation = source_query(query).expect("expected relation");
        let aggregation = aggregation_query(query).expect("expected aggregation");
        assert_eq!(aggregation.aggregate_slots.len(), 1, "outer select slots");

        let SourcePlan::Derived(derived) = relation else {
            panic!("expected derived table");
        };
        let inner_aggregation =
            aggregation_query(derived.query.as_ref()).expect("expected inner aggregation");

        assert_eq!(
            inner_aggregation.aggregate_slots.len(),
            1,
            "inner select slots"
        );
        assert_eq!(aggregation.aggregate_slots[0].slot, Some(0));
        assert_eq!(inner_aggregation.aggregate_slots[0].slot, Some(0));
    }

    #[test]
    fn binds_insert_and_create_table_source_queries() {
        let statement = parse_and_plan("INSERT INTO Target SELECT COUNT(*) FROM Source");
        let StatementPlan::Insert { source, .. } = statement else {
            panic!("expected insert");
        };
        assert_planned_query(&source);

        let statement = parse_and_plan("CREATE TABLE Target AS SELECT COUNT(*) FROM Source");
        let StatementPlan::CreateTable {
            source: Some(source),
            ..
        } = statement
        else {
            panic!("expected create table with source");
        };
        assert_planned_query(&source);
    }

    #[test]
    fn binds_update_and_delete_expr_subqueries() {
        let statement = parse_and_plan("UPDATE Target SET count = (SELECT COUNT(*) FROM Source)");
        let StatementPlan::Update { assignments, .. } = statement else {
            panic!("expected update");
        };
        let ExprPlan::Subquery(source) = &assignments[0].value else {
            panic!("expected assignment subquery");
        };
        assert_planned_query(source);

        let statement =
            parse_and_plan("UPDATE Target SET count = 1 WHERE id = (SELECT COUNT(*) FROM Source)");
        let StatementPlan::Update {
            selection: Some(ExprPlan::BinaryOp { right, .. }),
            ..
        } = statement
        else {
            panic!("expected update selection");
        };
        let ExprPlan::Subquery(source) = right.as_ref() else {
            panic!("expected selection subquery");
        };
        assert_planned_query(source);

        let statement =
            parse_and_plan("DELETE FROM Target WHERE id = (SELECT COUNT(*) FROM Source)");
        let StatementPlan::Delete {
            selection: Some(ExprPlan::BinaryOp { right, .. }),
            ..
        } = statement
        else {
            panic!("expected delete selection");
        };
        let ExprPlan::Subquery(source) = right.as_ref() else {
            panic!("expected delete subquery");
        };
        assert_planned_query(source);
    }

    #[test]
    fn keeps_create_table_without_source_unplanned() {
        let statement = parse_and_plan("CREATE TABLE Target (id INTEGER)");
        let StatementPlan::CreateTable { source, .. } = statement else {
            panic!("expected create table");
        };

        assert!(source.is_none());
    }

    #[test]
    fn keeps_non_query_statements_unchanged() {
        let statement = StatementPlan::ShowColumns {
            table_name: "Target".to_owned(),
        };

        assert_eq!(plan(statement.clone()), statement);
    }

    #[test]
    fn plans_values_limit_and_offset_subqueries() {
        let body_query = parse_query("SELECT id FROM Item");
        let body = match body_query {
            QueryPlan::Project(body) => Some(body),
            _ => None,
        }
        .expect("expected select");
        let offset = OffsetPlan {
            input: OffsetInputPlan::Project(body),
            count: subquery_expr(),
        };
        let query = QueryPlan::Limit(LimitPlan {
            input: LimitInputPlan::Offset(offset),
            count: subquery_expr(),
        });

        let statement = plan(StatementPlan::Query(query));
        let StatementPlan::Query(query) = statement else {
            panic!("expected query");
        };

        assert!(matches!(
            query,
            QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    count: ExprPlan::Subquery(offset),
                    ..
                }),
                count: ExprPlan::Subquery(limit),
            }) if {
                assert_planned_query(limit.as_ref());
                assert_planned_query(offset.as_ref());
                true
            }
        ));

        let statement = parse_and_plan("VALUES ((SELECT COUNT(*) FROM Item))");
        let StatementPlan::Query(query) = statement else {
            panic!("expected query");
        };
        let QueryPlan::Values(values) = query else {
            panic!("expected values");
        };
        let ExprPlan::Subquery(value_subquery) = &values.0[0][0] else {
            panic!("expected value subquery");
        };
        assert_planned_query(value_subquery);
    }

    #[test]
    fn plans_selection_group_by_and_in_subquery_exprs() {
        let statement = parse_and_plan(
            "
            SELECT id
            FROM Item
            WHERE EXISTS (SELECT COUNT(*) FROM Source)
            GROUP BY id IN (SELECT COUNT(*) FROM Source)
        ",
        );
        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        let filter = filter_query(query).expect("expected filter");
        let ExprPlan::Exists { subquery, .. } = &filter.expr else {
            panic!("expected exists selection");
        };
        assert_planned_query(subquery);

        let aggregation = aggregation_query(query).expect("expected aggregation");
        let ExprPlan::InSubquery { subquery, .. } = &aggregation.group_by[0] else {
            panic!("expected in-subquery group by");
        };
        assert_planned_query(subquery);
    }

    #[test]
    fn keeps_select_without_aggregates_unplanned() {
        let statement = parse_and_plan("SELECT * FROM Item");
        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        assert_unplanned_query(query);
    }

    #[test]
    fn preserves_explicit_aggregation_and_having_stages_without_slots() {
        let statement = parse_and_plan("SELECT category FROM Item GROUP BY category");
        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        let aggregation = aggregation_query(query).expect("expected aggregation");
        assert_eq!(
            aggregation.group_by,
            vec![ExprPlan::Identifier("category".to_owned())]
        );
        assert_eq!(aggregation.aggregate_slots, Vec::new());
        assert_eq!(having_query(query), None);

        let statement = parse_and_plan("SELECT 1 FROM Item HAVING TRUE");
        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        let aggregation = aggregation_query(query).expect("expected aggregation");
        let having = having_query(query).expect("expected having");
        assert_eq!(aggregation.group_by, Vec::new());
        assert_eq!(aggregation.aggregate_slots, Vec::new());
        assert_eq!(having.expr, ExprPlan::Value(Value::Bool(true)));
    }

    #[test]
    fn promotes_aggregate_only_projection_and_order_by() {
        let statement = parse_and_plan("SELECT COUNT(*) FROM Item");
        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        assert_eq!(
            aggregation_query(query)
                .expect("expected aggregation")
                .aggregate_slots
                .len(),
            1
        );

        let statement = parse_and_plan("SELECT id FROM Item ORDER BY COUNT(*)");
        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        assert_eq!(
            aggregation_query(query)
                .expect("expected aggregation")
                .aggregate_slots
                .len(),
            1
        );
    }

    #[test]
    fn binds_aggregate_used_only_by_having() {
        let statement = parse_and_plan("SELECT 1 FROM Item HAVING COUNT(*) > 0");
        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        let having = having_query(query).expect("expected having");

        assert_eq!(having.input.aggregate_slots.len(), 1);
        let mut slots = Vec::new();
        try_visit_expr(&having.expr, &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                slots.push(aggregate.slot);
            }
            Ok(())
        })
        .expect("having traversal");
        assert_eq!(slots, vec![Some(0)]);
    }

    #[test]
    fn keeps_schemaless_projection_unplanned() {
        let query = QueryPlan::Project(ProjectPlan {
            projection: ProjectionPlan::SchemalessMap,
            input: ProjectInputPlan::Source(SourcePlan::Dictionary(DictionarySourcePlan {
                dictionary: Dictionary::GlueTables,
                alias: alias("GLUE_TABLES"),
            })),
        });

        let StatementPlan::Query(query) = plan(StatementPlan::Query(query)) else {
            panic!("expected query");
        };
        assert_unplanned_query(&query);
    }

    #[test]
    fn plans_source_join_and_hash_executor_exprs() {
        let first_join = InnerJoinPlan {
            input: InnerJoinInputPlan::Condition(JoinConditionPlan {
                input: JoinConditionInputPlan::Hash(HashJoinPlan {
                    input: HashJoinInputPlan::Source(SourcePlan::Derived(DerivedSourcePlan {
                        query: Box::new(count_query()),
                        alias: alias("derived"),
                    })),
                    right: SourcePlan::Series(SeriesSourcePlan {
                        alias: alias("series"),
                        size: subquery_expr(),
                    }),
                    input_key: subquery_expr(),
                    right_key: subquery_expr(),
                    right_filter: Some(subquery_expr()),
                }),
                expr: subquery_expr(),
            }),
        };
        let second_join = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::Hash(HashJoinPlan {
                input: HashJoinInputPlan::InnerJoin(Box::new(first_join)),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "Target".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                }),
                input_key: subquery_expr(),
                right_key: subquery_expr(),
                right_filter: None,
            }),
        };
        let third_join = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(second_join)),
                right: SourcePlan::Dictionary(DictionarySourcePlan {
                    dictionary: Dictionary::GlueIndexes,
                    alias: alias("GLUE_INDEXES"),
                }),
            }),
        };
        let query = QueryPlan::SelectOrderBy(SelectOrderByPlan {
            input: ProjectPlan {
                projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
                input: ProjectInputPlan::InnerJoin(Box::new(third_join)),
            },
            exprs: vec![OrderByExprPlan {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
                asc: None,
            }],
        });

        let StatementPlan::Query(query) = plan(StatementPlan::Query(query)) else {
            panic!("expected query");
        };
        let third_join = inner_join_query(&query).expect("expected third join");
        let InnerJoinInputPlan::NestedLoop(third_nested_loop) = &third_join.input else {
            panic!("expected third nested loop");
        };
        let NestedLoopJoinInputPlan::LeftOuterJoin(second_join) = &third_nested_loop.input else {
            panic!("expected second join");
        };
        let LeftOuterJoinInputPlan::Hash(second_hash) = &second_join.input else {
            panic!("expected second hash join");
        };
        let HashJoinInputPlan::InnerJoin(first_join) = &second_hash.input else {
            panic!("expected first join");
        };
        let InnerJoinInputPlan::Condition(first_condition) = &first_join.input else {
            panic!("expected first condition");
        };
        let JoinConditionInputPlan::Hash(first_hash) = &first_condition.input else {
            panic!("expected first hash join");
        };
        let HashJoinInputPlan::Source(SourcePlan::Derived(derived)) = &first_hash.input else {
            panic!("expected derived relation");
        };
        assert_planned_query(derived.query.as_ref());

        let SourcePlan::Series(series) = &first_hash.right else {
            panic!("expected series relation");
        };
        let ExprPlan::Subquery(series_size) = &series.size else {
            panic!("expected series size subquery");
        };
        assert_planned_query(series_size);

        let ExprPlan::Subquery(join_on) = &first_condition.expr else {
            panic!("expected join on subquery");
        };
        assert_planned_query(join_on);

        let Some(right_filter) = &first_hash.right_filter else {
            panic!("expected hash right filter");
        };
        for expr in [&first_hash.right_key, &first_hash.input_key, right_filter] {
            let ExprPlan::Subquery(query) = expr else {
                panic!("expected hash executor subquery");
            };
            assert_planned_query(query);
        }

        assert_eq!(second_hash.right_filter, None);
        assert!(matches!(
            third_join.input,
            InnerJoinInputPlan::NestedLoop(_)
        ));
    }
}
