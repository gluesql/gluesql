use {
    crate::{
        ast::{
            Expr, Join, JoinConstraint, JoinExecutor, JoinOperator, OrderByExpr, Projection, Query,
            Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Values,
        },
        plan::expr::visit_mut_expr,
    },
    std::collections::HashMap,
};

pub fn plan(statement: Statement) -> Statement {
    match statement {
        Statement::Query(mut query) => {
            plan_query(&mut query);
            Statement::Query(query)
        }
        Statement::Insert {
            table_name,
            columns,
            mut source,
        } => {
            plan_query(&mut source);
            Statement::Insert {
                table_name,
                columns,
                source,
            }
        }
        Statement::CreateTable {
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

            Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                source,
                engine,
                foreign_keys,
                comment,
            }
        }
        Statement::Update {
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

            Statement::Update {
                table_name,
                assignments,
                selection,
            }
        }
        Statement::Delete {
            table_name,
            mut selection,
        } => {
            if let Some(selection) = selection.as_mut() {
                plan_expr(selection);
            }

            Statement::Delete {
                table_name,
                selection,
            }
        }
        _ => statement,
    }
}

fn plan_query(query: &mut Query) {
    match &mut query.body {
        SetExpr::Select(select) => {
            plan_select(select);

            for order_by in &mut query.order_by {
                plan_expr(&mut order_by.expr);
            }

            bind_select(select, &mut query.order_by);
        }
        SetExpr::Values(Values(exprs_list)) => {
            for exprs in exprs_list {
                for expr in exprs {
                    plan_expr(expr);
                }
            }
        }
    }

    if let Some(limit) = query.limit.as_mut() {
        plan_expr(limit);
    }

    if let Some(offset) = query.offset.as_mut() {
        plan_expr(offset);
    }
}

fn plan_select(select: &mut Select) {
    plan_table_with_joins(&mut select.from);

    match &mut select.projection {
        Projection::SelectItems(items) => {
            for item in items {
                if let SelectItem::Expr { expr, .. } = item {
                    plan_expr(expr);
                }
            }
        }
        Projection::SchemalessMap => {}
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

fn plan_table_with_joins(table_with_joins: &mut TableWithJoins) {
    plan_table_factor(&mut table_with_joins.relation);

    for join in &mut table_with_joins.joins {
        plan_join(join);
    }
}

fn plan_table_factor(table_factor: &mut TableFactor) {
    match table_factor {
        TableFactor::Table { .. } | TableFactor::Dictionary { .. } => {}
        TableFactor::Derived { subquery, .. } => plan_query(subquery),
        TableFactor::Series { size, .. } => plan_expr(size),
    }
}

fn plan_join(join: &mut Join) {
    plan_table_factor(&mut join.relation);

    match &mut join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr))
        | JoinOperator::LeftOuter(JoinConstraint::On(expr)) => plan_expr(expr),
        JoinOperator::Inner(JoinConstraint::None)
        | JoinOperator::LeftOuter(JoinConstraint::None) => {}
    }

    if let JoinExecutor::Hash {
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

fn plan_expr(expr: &mut Expr) {
    visit_mut_expr(expr, &mut |expr| match expr {
        Expr::Subquery(subquery)
        | Expr::Exists { subquery, .. }
        | Expr::InSubquery { subquery, .. } => plan_query(subquery),
        _ => {}
    });
}

fn bind_select(select: &mut Select, order_by: &mut [OrderByExpr]) {
    let mut slots = HashMap::new();
    let mut aggregates = Vec::new();
    let mut bind = |expr: &mut Expr| {
        visit_mut_expr(expr, &mut |expr| {
            if let Expr::Aggregate(aggregate) = expr {
                let slot = *slots.entry(aggregate.as_ref().clone()).or_insert_with(|| {
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

    if let Projection::SelectItems(items) = &mut select.projection {
        for item in items {
            if let SelectItem::Expr { expr, .. } = item {
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
            ast::{
                Dictionary, Expr, Join, JoinConstraint, JoinExecutor, JoinOperator, Literal,
                OrderByExpr, Projection, Query, Select, SelectItem, SetExpr, Statement, TableAlias,
                TableFactor, TableWithJoins,
            },
            parse_sql::parse,
            plan::expr::try_visit_expr,
            translate::translate,
        },
    };

    fn parse_and_plan(sql: &str) -> Statement {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        let translated = translate(&parsed).expect(sql);

        plan(translated)
    }

    fn parse_query(sql: &str) -> Query {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        let Statement::Query(query) = translate(&parsed).expect(sql) else {
            panic!("expected query");
        };

        query
    }

    fn select(statement: &Statement) -> &Select {
        let Statement::Query(query) = statement else {
            panic!("expected query");
        };
        select_query(query)
    }

    fn select_query(query: &Query) -> &Select {
        let SetExpr::Select(select) = &query.body else {
            panic!("expected select");
        };

        select
    }

    fn assert_planned_query(query: &Query) {
        assert_eq!(
            select_query(query).aggregate_slots.as_ref().map(Vec::len),
            Some(1)
        );
    }

    fn assert_unplanned_query(query: &Query) {
        assert_eq!(select_query(query).aggregate_slots, None);
    }

    fn count_query() -> Query {
        parse_query("SELECT COUNT(*) FROM Item")
    }

    fn subquery_expr() -> Expr {
        Expr::Subquery(Box::new(count_query()))
    }

    fn alias(name: &str) -> TableAlias {
        TableAlias {
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

        let Projection::SelectItems(items) = &select.projection else {
            panic!("expected select items");
        };
        let SelectItem::Expr { expr, .. } = &items[0] else {
            panic!("expected expression");
        };

        let mut found_slots = Vec::new();
        try_visit_expr(expr, &mut |expr| {
            if let Expr::Aggregate(aggregate) = expr {
                found_slots.push(aggregate.slot);
            }

            Ok(())
        })
        .expect("projection traversal");
        try_visit_expr(select.having.as_ref().expect("having"), &mut |expr| {
            if let Expr::Aggregate(aggregate) = expr {
                found_slots.push(aggregate.slot);
            }

            Ok(())
        })
        .expect("having traversal");

        let Statement::Query(query) = &statement else {
            panic!("expected query");
        };
        try_visit_expr(&query.order_by[0].expr, &mut |expr| {
            if let Expr::Aggregate(aggregate) = expr {
                found_slots.push(aggregate.slot);
            }
            Ok(())
        })
        .expect("order by traversal");

        assert_eq!(found_slots, vec![Some(0), Some(0), Some(0)]);
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

        let TableFactor::Derived { subquery, .. } = &select.from.relation else {
            panic!("expected derived table");
        };
        let SetExpr::Select(inner) = &subquery.body else {
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
        let Statement::Insert { source, .. } = statement else {
            panic!("expected insert");
        };
        assert_planned_query(&source);

        let statement = parse_and_plan("CREATE TABLE Target AS SELECT COUNT(*) FROM Source");
        let Statement::CreateTable {
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
        let Statement::Update { assignments, .. } = statement else {
            panic!("expected update");
        };
        let Expr::Subquery(source) = &assignments[0].value else {
            panic!("expected assignment subquery");
        };
        assert_planned_query(source);

        let statement =
            parse_and_plan("UPDATE Target SET count = 1 WHERE id = (SELECT COUNT(*) FROM Source)");
        let Statement::Update {
            selection: Some(Expr::BinaryOp { right, .. }),
            ..
        } = statement
        else {
            panic!("expected update selection");
        };
        let Expr::Subquery(source) = right.as_ref() else {
            panic!("expected selection subquery");
        };
        assert_planned_query(source);

        let statement =
            parse_and_plan("DELETE FROM Target WHERE id = (SELECT COUNT(*) FROM Source)");
        let Statement::Delete {
            selection: Some(Expr::BinaryOp { right, .. }),
            ..
        } = statement
        else {
            panic!("expected delete selection");
        };
        let Expr::Subquery(source) = right.as_ref() else {
            panic!("expected delete subquery");
        };
        assert_planned_query(source);
    }

    #[test]
    fn keeps_create_table_without_source_unplanned() {
        let statement = parse_and_plan("CREATE TABLE Target (id INTEGER)");
        let Statement::CreateTable { source, .. } = statement else {
            panic!("expected create table");
        };

        assert!(source.is_none());
    }

    #[test]
    fn keeps_non_query_statements_unchanged() {
        let statement = Statement::ShowColumns {
            table_name: "Target".to_owned(),
        };

        assert_eq!(plan(statement.clone()), statement);
    }

    #[test]
    fn plans_values_limit_and_offset_subqueries() {
        let mut query = parse_query("SELECT id FROM Item");
        query.limit = Some(subquery_expr());
        query.offset = Some(subquery_expr());

        let statement = plan(Statement::Query(query));
        let Statement::Query(query) = statement else {
            panic!("expected query");
        };

        let Some(Expr::Subquery(limit)) = &query.limit else {
            panic!("expected limit subquery");
        };
        assert_planned_query(limit);

        let Some(Expr::Subquery(offset)) = &query.offset else {
            panic!("expected offset subquery");
        };
        assert_planned_query(offset);

        let statement = parse_and_plan("VALUES ((SELECT COUNT(*) FROM Item))");
        let Statement::Query(query) = statement else {
            panic!("expected query");
        };
        let SetExpr::Values(values) = &query.body else {
            panic!("expected values");
        };
        let Expr::Subquery(value_subquery) = &values.0[0][0] else {
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

        let Some(Expr::Exists { subquery, .. }) = &select.selection else {
            panic!("expected exists selection");
        };
        assert_planned_query(subquery);

        let Expr::InSubquery { subquery, .. } = &select.group_by[0] else {
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
        let query = Query {
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                projection: Projection::SchemalessMap,
                from: TableWithJoins {
                    relation: TableFactor::Dictionary {
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
            limit: None,
            offset: None,
        };

        let Statement::Query(query) = plan(Statement::Query(query)) else {
            panic!("expected query");
        };
        assert_unplanned_query(&query);
    }

    #[test]
    fn plans_table_factor_join_and_hash_executor_exprs() {
        let query = Query {
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                projection: Projection::SelectItems(vec![SelectItem::Wildcard]),
                from: TableWithJoins {
                    relation: TableFactor::Derived {
                        subquery: count_query(),
                        alias: alias("derived"),
                    },
                    joins: vec![
                        Join {
                            relation: TableFactor::Series {
                                alias: alias("series"),
                                size: subquery_expr(),
                            },
                            join_operator: JoinOperator::Inner(JoinConstraint::On(subquery_expr())),
                            join_executor: JoinExecutor::Hash {
                                key_expr: subquery_expr(),
                                value_expr: subquery_expr(),
                                where_clause: Some(subquery_expr()),
                            },
                        },
                        Join {
                            relation: TableFactor::Table {
                                name: "Target".to_owned(),
                                alias: None,
                                index: None,
                            },
                            join_operator: JoinOperator::LeftOuter(JoinConstraint::None),
                            join_executor: JoinExecutor::Hash {
                                key_expr: subquery_expr(),
                                value_expr: subquery_expr(),
                                where_clause: None,
                            },
                        },
                        Join {
                            relation: TableFactor::Dictionary {
                                dict: Dictionary::GlueIndexes,
                                alias: alias("GLUE_INDEXES"),
                            },
                            join_operator: JoinOperator::Inner(JoinConstraint::None),
                            join_executor: JoinExecutor::NestedLoop,
                        },
                    ],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
                aggregate_slots: None,
            })),
            order_by: vec![OrderByExpr {
                expr: Expr::Literal(Literal::Number(1.into())),
                asc: None,
            }],
            limit: None,
            offset: None,
        };

        let Statement::Query(query) = plan(Statement::Query(query)) else {
            panic!("expected query");
        };
        let select = select_query(&query);

        let TableFactor::Derived { subquery, .. } = &select.from.relation else {
            panic!("expected derived relation");
        };
        assert_planned_query(subquery);

        let TableFactor::Series { size, .. } = &select.from.joins[0].relation else {
            panic!("expected series relation");
        };
        let Expr::Subquery(series_size) = size else {
            panic!("expected series size subquery");
        };
        assert_planned_query(series_size);

        let JoinOperator::Inner(JoinConstraint::On(Expr::Subquery(join_on))) =
            &select.from.joins[0].join_operator
        else {
            panic!("expected join on subquery");
        };
        assert_planned_query(join_on);

        let JoinExecutor::Hash {
            key_expr,
            value_expr,
            where_clause: Some(where_clause),
        } = &select.from.joins[0].join_executor
        else {
            panic!("expected hash executor");
        };

        for expr in [key_expr, value_expr, where_clause] {
            let Expr::Subquery(query) = expr else {
                panic!("expected hash executor subquery");
            };
            assert_planned_query(query);
        }

        let JoinOperator::LeftOuter(JoinConstraint::None) = &select.from.joins[1].join_operator
        else {
            panic!("expected left join without constraint");
        };
        let JoinExecutor::Hash {
            where_clause: None, ..
        } = &select.from.joins[1].join_executor
        else {
            panic!("expected hash executor without where clause");
        };

        let JoinOperator::Inner(JoinConstraint::None) = &select.from.joins[2].join_operator else {
            panic!("expected inner join without constraint");
        };
        assert!(matches!(
            select.from.joins[2].join_executor,
            JoinExecutor::NestedLoop
        ));
    }
}
