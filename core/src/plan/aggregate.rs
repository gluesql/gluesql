use {
    crate::plan::{
        AggregateFunctionPlan, AggregationPlan, DistinctInputPlan, DistinctPlan, ExprPlan,
        JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan, LimitInputPlan,
        LimitPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan, ProjectInputPlan, ProjectPlan,
        ProjectionPlan, QueryPlan, SelectItemPlan, SelectOrderByPlan, SelectPlan, StatementPlan,
        TableFactorPlan, TableWithJoinsPlan, ValuesOrderByPlan, ValuesPlan, expr::visit_mut_expr,
    },
    std::collections::HashMap,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggregateKey {
    func: AggregateFunctionPlan,
    distinct: bool,
}

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
        ProjectInputPlan::Select(select) => plan_select(select),
        ProjectInputPlan::Aggregation(aggregation) => {
            plan_select(&mut aggregation.input);
            for group_by in &mut aggregation.group_by {
                plan_expr(group_by);
            }
        }
        ProjectInputPlan::Having(having) => {
            plan_select(&mut having.input.input);
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

fn plan_select(select: &mut SelectPlan) {
    plan_table_with_joins(&mut select.from);

    if let Some(selection) = select.selection.as_mut() {
        plan_expr(selection);
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

fn plan_table_with_joins(table_with_joins: &mut TableWithJoinsPlan) {
    plan_table_factor(&mut table_with_joins.relation);

    for join in &mut table_with_joins.joins {
        plan_join(join);
    }
}

fn plan_table_factor(table_factor: &mut TableFactorPlan) {
    match table_factor {
        TableFactorPlan::Table { .. } | TableFactorPlan::Dictionary { .. } => {}
        TableFactorPlan::Derived { subquery, .. } => plan_query(subquery),
        TableFactorPlan::Series { size, .. } => plan_expr(size),
    }
}

fn plan_join(join: &mut JoinPlan) {
    plan_table_factor(&mut join.relation);

    match &mut join.join_operator {
        JoinOperatorPlan::Inner(JoinConstraintPlan::On(expr))
        | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::On(expr)) => plan_expr(expr),
        JoinOperatorPlan::Inner(JoinConstraintPlan::None)
        | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None) => {}
    }

    if let JoinExecutorPlan::Hash {
        key_expr,
        value_expr,
        where_clause,
    } = &mut join.join_executor
    {
        plan_expr(key_expr);
        plan_expr(value_expr);

        if let Some(where_clause) = where_clause {
            plan_expr(where_clause);
        }
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
        ProjectInputPlan::Select(select) if !aggregates.is_empty() => {
            *input = ProjectInputPlan::Aggregation(AggregationPlan {
                input: select.clone(),
                group_by: Vec::new(),
                aggregate_slots: aggregates,
            });
        }
        ProjectInputPlan::Select(_) => {}
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
            parse_sql::parse,
            plan::{
                AggregationPlan, DistinctInputPlan, DistinctPlan, ExprPlan, HavingPlan,
                JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan, LimitInputPlan,
                LimitPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan, ProjectInputPlan,
                ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan, SelectOrderByPlan,
                SelectPlan, StatementPlan, TableAliasPlan, TableFactorPlan, TableWithJoinsPlan,
                expr::{try_visit_expr, visit_mut_expr},
            },
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

    fn select(statement: &StatementPlan) -> &SelectPlan {
        let StatementPlan::Query(query) = statement else {
            panic!("expected query");
        };
        select_query(query).expect("expected select")
    }

    fn project(statement: &StatementPlan) -> &ProjectPlan {
        let StatementPlan::Query(query) = statement else {
            panic!("expected query");
        };
        project_query(query).expect("expected project")
    }

    fn offset_project(offset: &OffsetPlan) -> Option<&ProjectPlan> {
        match &offset.input {
            OffsetInputPlan::Project(project) => Some(project),
            OffsetInputPlan::SelectOrderBy(SelectOrderByPlan { input, .. }) => Some(input),
            OffsetInputPlan::Distinct(distinct) => Some(distinct_project(distinct)),
            OffsetInputPlan::Values(_) | OffsetInputPlan::ValuesOrderBy(_) => None,
        }
    }

    fn distinct_project(distinct: &DistinctPlan) -> &ProjectPlan {
        match &distinct.input {
            DistinctInputPlan::Project(project) => project,
            DistinctInputPlan::SelectOrderBy(SelectOrderByPlan { input, .. }) => input,
        }
    }

    fn project_query(query: &QueryPlan) -> Option<&ProjectPlan> {
        match query {
            QueryPlan::Project(project) => Some(project),
            QueryPlan::SelectOrderBy(SelectOrderByPlan { input, .. }) => Some(input),
            QueryPlan::Distinct(distinct) => Some(distinct_project(distinct)),
            QueryPlan::Offset(offset) => offset_project(offset),
            QueryPlan::Limit(LimitPlan { input, .. }) => match input {
                LimitInputPlan::Project(project) => Some(project),
                LimitInputPlan::SelectOrderBy(SelectOrderByPlan { input, .. }) => Some(input),
                LimitInputPlan::Distinct(distinct) => Some(distinct_project(distinct)),
                LimitInputPlan::Offset(offset) => offset_project(offset),
                LimitInputPlan::Values(_) | LimitInputPlan::ValuesOrderBy(_) => None,
            },
            QueryPlan::Values(_) | QueryPlan::ValuesOrderBy(_) => None,
        }
    }

    fn select_query(query: &QueryPlan) -> Option<&SelectPlan> {
        project_query(query).map(|project| match &project.input {
            ProjectInputPlan::Select(select) => select.as_ref(),
            ProjectInputPlan::Aggregation(aggregation) => aggregation.input.as_ref(),
            ProjectInputPlan::Having(having) => having.input.input.as_ref(),
        })
    }

    fn aggregation_query(query: &QueryPlan) -> Option<&AggregationPlan> {
        project_query(query).and_then(|project| match &project.input {
            ProjectInputPlan::Aggregation(aggregation) => Some(aggregation),
            ProjectInputPlan::Having(having) => Some(&having.input),
            ProjectInputPlan::Select(_) => None,
        })
    }

    fn having_query(query: &QueryPlan) -> Option<&HavingPlan> {
        project_query(query).and_then(|project| match &project.input {
            ProjectInputPlan::Having(having) => Some(having),
            ProjectInputPlan::Select(_) | ProjectInputPlan::Aggregation(_) => None,
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
        let project = project_query(query).expect("expected project");
        assert!(matches!(project.input, ProjectInputPlan::Select(_)));
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
        let mut project = project_query(&query).expect("expected project").clone();
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
            &project_query(&query).expect("expected project").projection
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
        let select = select_query(query).expect("expected select");
        let aggregation = aggregation_query(query).expect("expected aggregation");
        assert_eq!(aggregation.aggregate_slots.len(), 1, "outer select slots");

        let TableFactorPlan::Derived { subquery, .. } = &select.from.relation else {
            panic!("expected derived table");
        };
        let inner_aggregation = aggregation_query(subquery).expect("expected inner aggregation");

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
        let select = select(&statement);

        let Some(ExprPlan::Exists { subquery, .. }) = &select.selection else {
            panic!("expected exists selection");
        };
        assert_planned_query(subquery);

        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
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
        assert_eq!(having.expr, ExprPlan::Value(crate::data::Value::Bool(true)));
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
            input: ProjectInputPlan::Select(Box::new(SelectPlan {
                from: TableWithJoinsPlan {
                    relation: TableFactorPlan::Dictionary {
                        dict: Dictionary::GlueTables,
                        alias: alias("GLUE_TABLES"),
                    },
                    joins: Vec::new(),
                },
                selection: None,
            })),
        });

        let StatementPlan::Query(query) = plan(StatementPlan::Query(query)) else {
            panic!("expected query");
        };
        assert_unplanned_query(&query);
    }

    #[test]
    fn plans_table_factor_join_and_hash_executor_exprs() {
        let query = QueryPlan::SelectOrderBy(SelectOrderByPlan {
            input: ProjectPlan {
                projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
                input: ProjectInputPlan::Select(Box::new(SelectPlan {
                    from: TableWithJoinsPlan {
                        relation: TableFactorPlan::Derived {
                            subquery: count_query(),
                            alias: alias("derived"),
                        },
                        joins: vec![
                            JoinPlan {
                                relation: TableFactorPlan::Series {
                                    alias: alias("series"),
                                    size: subquery_expr(),
                                },
                                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::On(
                                    subquery_expr(),
                                )),
                                join_executor: JoinExecutorPlan::Hash {
                                    key_expr: subquery_expr(),
                                    value_expr: subquery_expr(),
                                    where_clause: Some(subquery_expr()),
                                },
                            },
                            JoinPlan {
                                relation: TableFactorPlan::Table {
                                    name: "Target".to_owned(),
                                    alias: None,
                                    index: None,
                                },
                                join_operator: JoinOperatorPlan::LeftOuter(
                                    JoinConstraintPlan::None,
                                ),
                                join_executor: JoinExecutorPlan::Hash {
                                    key_expr: subquery_expr(),
                                    value_expr: subquery_expr(),
                                    where_clause: None,
                                },
                            },
                            JoinPlan {
                                relation: TableFactorPlan::Dictionary {
                                    dict: Dictionary::GlueIndexes,
                                    alias: alias("GLUE_INDEXES"),
                                },
                                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                                join_executor: JoinExecutorPlan::NestedLoop,
                            },
                        ],
                    },
                    selection: None,
                })),
            },
            exprs: vec![OrderByExprPlan {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
                asc: None,
            }],
        });

        let StatementPlan::Query(query) = plan(StatementPlan::Query(query)) else {
            panic!("expected query");
        };
        let select = select_query(&query).expect("expected select");

        let TableFactorPlan::Derived { subquery, .. } = &select.from.relation else {
            panic!("expected derived relation");
        };
        assert_planned_query(subquery);

        let TableFactorPlan::Series { size, .. } = &select.from.joins[0].relation else {
            panic!("expected series relation");
        };
        let ExprPlan::Subquery(series_size) = size else {
            panic!("expected series size subquery");
        };
        assert_planned_query(series_size);

        let JoinOperatorPlan::Inner(JoinConstraintPlan::On(ExprPlan::Subquery(join_on))) =
            &select.from.joins[0].join_operator
        else {
            panic!("expected join on subquery");
        };
        assert_planned_query(join_on);

        let JoinExecutorPlan::Hash {
            key_expr,
            value_expr,
            where_clause: Some(where_clause),
        } = &select.from.joins[0].join_executor
        else {
            panic!("expected hash executor");
        };

        for expr in [key_expr, value_expr, where_clause] {
            let ExprPlan::Subquery(query) = expr else {
                panic!("expected hash executor subquery");
            };
            assert_planned_query(query);
        }

        let JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None) =
            &select.from.joins[1].join_operator
        else {
            panic!("expected left join without constraint");
        };
        let JoinExecutorPlan::Hash {
            where_clause: None, ..
        } = &select.from.joins[1].join_executor
        else {
            panic!("expected hash executor without where clause");
        };

        let JoinOperatorPlan::Inner(JoinConstraintPlan::None) = &select.from.joins[2].join_operator
        else {
            panic!("expected inner join without constraint");
        };
        assert!(matches!(
            select.from.joins[2].join_executor,
            JoinExecutorPlan::NestedLoop
        ));
    }
}
