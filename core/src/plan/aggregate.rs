use {
    crate::plan::{
        AggregateFunctionPlan, ExprPlan, JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan,
        JoinPlan, LimitInputPlan, LimitPlan, OffsetPlan, OrderByExprPlan, ProjectionPlan,
        QueryBodyPlan, QueryPlan, SelectItemPlan, SelectPlan, SetExprPlan, StatementPlan,
        TableFactorPlan, TableWithJoinsPlan, ValuesPlan, expr::visit_mut_expr,
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
        QueryPlan::Body(body) => plan_query_body(body),
        QueryPlan::Offset(OffsetPlan { input, count }) => {
            plan_query_body(input);
            plan_expr(count);
        }
        QueryPlan::Limit(LimitPlan { input, count }) => {
            match input {
                LimitInputPlan::Body(body) => plan_query_body(body),
                LimitInputPlan::Offset(OffsetPlan { input, count }) => {
                    plan_query_body(input);
                    plan_expr(count);
                }
            }

            plan_expr(count);
        }
    }
}

fn plan_query_body(body_plan: &mut QueryBodyPlan) {
    match &mut body_plan.body {
        SetExprPlan::Select(select) => {
            plan_select(select);

            for order_by in &mut body_plan.order_by {
                plan_expr(&mut order_by.expr);
            }

            bind_select(select, &mut body_plan.order_by);
        }
        SetExprPlan::Values(ValuesPlan(exprs_list)) => {
            for exprs in exprs_list {
                for expr in exprs {
                    plan_expr(expr);
                }
            }
        }
    }
}

fn plan_select(select: &mut SelectPlan) {
    plan_table_with_joins(&mut select.from);

    match &mut select.projection {
        ProjectionPlan::SelectItems(items) => {
            for item in items {
                if let SelectItemPlan::Expr { expr, .. } = item {
                    plan_expr(expr);
                }
            }
        }
        ProjectionPlan::SchemalessMap => {}
    }

    if let Some(selection) = select.selection.as_mut() {
        plan_expr(selection);
    }

    for group_by in &mut select.group_by {
        plan_expr(group_by);
    }

    if let Some(having) = select.having.as_mut() {
        plan_expr(having);
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

fn bind_select(select: &mut SelectPlan, order_by: &mut [OrderByExprPlan]) {
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

    if let ProjectionPlan::SelectItems(items) = &mut select.projection {
        for item in items {
            if let SelectItemPlan::Expr { expr, .. } = item {
                bind(expr);
            }
        }
    }

    if let Some(having) = select.having.as_mut() {
        bind(having);
    }

    for order_by in order_by {
        bind(&mut order_by.expr);
    }

    select.aggregate_slots = (!aggregates.is_empty()).then_some(aggregates);
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            ast::{Dictionary, Literal},
            parse_sql::parse,
            plan::{
                ExprPlan, JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan,
                LimitInputPlan, LimitPlan, OffsetPlan, OrderByExprPlan, ProjectionPlan,
                QueryBodyPlan, QueryPlan, SelectItemPlan, SelectPlan, SetExprPlan, StatementPlan,
                TableAliasPlan, TableFactorPlan, TableWithJoinsPlan,
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
        select_query(query)
    }

    fn select_query(query: &QueryPlan) -> &SelectPlan {
        let QueryPlan::Body(body) = query else {
            panic!("expected query body");
        };
        let SetExprPlan::Select(select) = &body.body else {
            panic!("expected select");
        };

        select
    }

    fn assert_planned_query(query: &QueryPlan) {
        assert_eq!(
            select_query(query).aggregate_slots.as_ref().map(Vec::len),
            Some(1)
        );
    }

    fn assert_unplanned_query(query: &QueryPlan) {
        assert_eq!(select_query(query).aggregate_slots, None);
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
        let select = select(&statement);

        let slots = select
            .aggregate_slots
            .as_ref()
            .expect("aggregate slots should be planned");
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].slot, Some(0));

        let ProjectionPlan::SelectItems(items) = &select.projection else {
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
        try_visit_expr(select.having.as_ref().expect("having"), &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                found_slots.push(aggregate.slot);
            }

            Ok(())
        })
        .expect("having traversal");

        let StatementPlan::Query(query) = &statement else {
            panic!("expected query");
        };
        let QueryPlan::Body(body) = query else {
            panic!("expected query body");
        };
        try_visit_expr(&body.order_by[0].expr, &mut |expr| {
            if let ExprPlan::Aggregate(aggregate) = expr {
                found_slots.push(aggregate.slot);
            }
            Ok(())
        })
        .expect("order by traversal");

        assert_eq!(found_slots, vec![Some(0), Some(0), Some(0)]);
    }

    #[test]
    fn ignores_stale_slot_when_binding_same_aggregate() {
        let mut query = parse_query("SELECT COUNT(*) FROM Item HAVING COUNT(*) > 0");
        let QueryPlan::Body(body) = &mut query else {
            panic!("expected query body");
        };
        let SetExprPlan::Select(select) = &mut body.body else {
            panic!("expected select");
        };
        let ProjectionPlan::SelectItems(items) = &mut select.projection else {
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

        let StatementPlan::Query(query) = plan(StatementPlan::Query(query)) else {
            panic!("expected query");
        };
        let select = select_query(&query);
        let slots = select
            .aggregate_slots
            .as_ref()
            .expect("aggregate slots should be planned");

        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].slot, Some(0));

        let ProjectionPlan::SelectItems(items) = &select.projection else {
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
        try_visit_expr(select.having.as_ref().expect("having"), &mut |expr| {
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
        let select = select(&statement);
        assert_eq!(
            select.aggregate_slots.as_ref().map(Vec::len),
            Some(1),
            "outer select slots"
        );

        let TableFactorPlan::Derived { subquery, .. } = &select.from.relation else {
            panic!("expected derived table");
        };
        let QueryPlan::Body(body) = subquery else {
            panic!("expected query body");
        };
        let SetExprPlan::Select(inner) = &body.body else {
            panic!("expected inner select");
        };

        assert_eq!(
            inner.aggregate_slots.as_ref().map(Vec::len),
            Some(1),
            "inner select slots"
        );
        assert_eq!(select.aggregate_slots.as_ref().unwrap()[0].slot, Some(0));
        assert_eq!(inner.aggregate_slots.as_ref().unwrap()[0].slot, Some(0));
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
        let QueryPlan::Body(body) = parse_query("SELECT id FROM Item") else {
            panic!("expected query body");
        };
        let offset = OffsetPlan {
            input: body,
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

        let QueryPlan::Limit(LimitPlan { input, count }) = query else {
            panic!("expected limit plan");
        };
        let ExprPlan::Subquery(limit) = count else {
            panic!("expected limit subquery");
        };
        assert_planned_query(limit.as_ref());

        let LimitInputPlan::Offset(OffsetPlan { count, .. }) = input else {
            panic!("expected offset plan");
        };
        let ExprPlan::Subquery(offset) = count else {
            panic!("expected offset subquery");
        };
        assert_planned_query(offset.as_ref());

        let statement = parse_and_plan("VALUES ((SELECT COUNT(*) FROM Item))");
        let StatementPlan::Query(query) = statement else {
            panic!("expected query");
        };
        let QueryPlan::Body(body) = query else {
            panic!("expected query body");
        };
        let SetExprPlan::Values(values) = &body.body else {
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

        let ExprPlan::InSubquery { subquery, .. } = &select.group_by[0] else {
            panic!("expected in-subquery group by");
        };
        assert_planned_query(subquery);
    }

    #[test]
    fn keeps_select_without_aggregates_unplanned() {
        let statement = parse_and_plan("SELECT * FROM Item");
        let select = select(&statement);

        assert_eq!(select.aggregate_slots, None);
    }

    #[test]
    fn keeps_schemaless_projection_unplanned() {
        let query = QueryPlan::Body(QueryBodyPlan {
            body: SetExprPlan::Select(Box::new(SelectPlan {
                distinct: false,
                projection: ProjectionPlan::SchemalessMap,
                from: TableWithJoinsPlan {
                    relation: TableFactorPlan::Dictionary {
                        dict: Dictionary::GlueTables,
                        alias: alias("GLUE_TABLES"),
                    },
                    joins: Vec::new(),
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
                aggregate_slots: None,
            })),
            order_by: Vec::new(),
        });

        let StatementPlan::Query(query) = plan(StatementPlan::Query(query)) else {
            panic!("expected query");
        };
        assert_unplanned_query(&query);
    }

    #[test]
    fn plans_table_factor_join_and_hash_executor_exprs() {
        let query = QueryPlan::Body(QueryBodyPlan {
            body: SetExprPlan::Select(Box::new(SelectPlan {
                distinct: false,
                projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
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
                            join_operator: JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None),
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
                group_by: Vec::new(),
                having: None,
                aggregate_slots: None,
            })),
            order_by: vec![OrderByExprPlan {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
                asc: None,
            }],
        });

        let StatementPlan::Query(query) = plan(StatementPlan::Query(query)) else {
            panic!("expected query");
        };
        let select = select_query(&query);

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
