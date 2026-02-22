use {
    super::super::{PlanError, expr::visit_mut_expr},
    crate::{
        ast::{
            Expr, JoinConstraint, JoinExecutor, JoinOperator, Projection, Query, Select,
            SelectItem, SetExpr, Statement, TableFactor,
        },
        data::Schema,
        result::Result,
    },
    std::{collections::HashMap, hash::BuildHasher, iter::once},
};

/// Rejects only mixed schemaful/schemaless join wildcard projections.
pub(super) fn validate_statement<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: &Statement,
) -> Result<()> {
    match statement {
        Statement::Query(query) => validate_query(schema_map, query),
        Statement::Insert { source, .. } => validate_query(schema_map, source),
        Statement::CreateTable { source, .. } => source
            .as_ref()
            .map_or(Ok(()), |query| validate_query(schema_map, query)),
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            for assignment in assignments {
                validate_expr(schema_map, &assignment.value)?;
            }

            selection
                .as_ref()
                .map_or(Ok(()), |expr| validate_expr(schema_map, expr))
        }
        Statement::Delete { selection, .. } => selection
            .as_ref()
            .map_or(Ok(()), |expr| validate_expr(schema_map, expr)),
        _ => Ok(()),
    }
}

fn validate_query(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    query: &Query,
) -> Result<()> {
    match &query.body {
        SetExpr::Select(select) => validate_select(schema_map, select)?,
        SetExpr::Values(values) => {
            for row in &values.0 {
                for expr in row {
                    validate_expr(schema_map, expr)?;
                }
            }
        }
    }

    for order_by in &query.order_by {
        validate_expr(schema_map, &order_by.expr)?;
    }

    if let Some(limit) = &query.limit {
        validate_expr(schema_map, limit)?;
    }

    if let Some(offset) = &query.offset {
        validate_expr(schema_map, offset)?;
    }

    Ok(())
}

fn validate_select(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    select: &Select,
) -> Result<()> {
    validate_mixed_join_wildcard_projection(schema_map, select)?;
    validate_table_factor(schema_map, &select.from.relation)?;

    for join in &select.from.joins {
        validate_table_factor(schema_map, &join.relation)?;

        match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr))
            | JoinOperator::LeftOuter(JoinConstraint::On(expr)) => {
                validate_expr(schema_map, expr)?;
            }
            _ => {}
        }

        if let JoinExecutor::Hash {
            key_expr,
            value_expr,
            where_clause,
        } = &join.join_executor
        {
            validate_expr(schema_map, key_expr)?;
            validate_expr(schema_map, value_expr)?;
            if let Some(expr) = where_clause {
                validate_expr(schema_map, expr)?;
            }
        }
    }

    if let Projection::SelectItems(projection) = &select.projection {
        for projection in projection {
            if let SelectItem::Expr { expr, .. } = projection {
                validate_expr(schema_map, expr)?;
            }
        }
    }

    if let Some(selection) = &select.selection {
        validate_expr(schema_map, selection)?;
    }

    for group_by in &select.group_by {
        validate_expr(schema_map, group_by)?;
    }

    if let Some(having) = &select.having {
        validate_expr(schema_map, having)?;
    }

    Ok(())
}

fn validate_table_factor(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    table_factor: &TableFactor,
) -> Result<()> {
    match table_factor {
        TableFactor::Derived { subquery, .. } => validate_query(schema_map, subquery),
        _ => Ok(()),
    }
}

fn validate_expr(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    expr: &Expr,
) -> Result<()> {
    let mut expr = expr.clone();
    let mut validation = Ok(());

    visit_mut_expr(&mut expr, &mut |expr| {
        if validation.is_err() {
            return;
        }

        match expr {
            Expr::Subquery(subquery)
            | Expr::Exists { subquery, .. }
            | Expr::InSubquery { subquery, .. } => {
                validation = validate_query(schema_map, subquery);
            }
            _ => {}
        }
    });

    validation
}

fn validate_mixed_join_wildcard_projection(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    select: &Select,
) -> Result<()> {
    if select.from.joins.is_empty()
        || !matches!(
            &select.projection,
            Projection::SelectItems(projection)
                if projection
                    .iter()
                    .any(|item| matches!(item, SelectItem::Wildcard))
        )
    {
        return Ok(());
    }

    let mut has_schemaless = false;
    let mut has_schemaful = false;

    for relation in
        once(&select.from.relation).chain(select.from.joins.iter().map(|join| &join.relation))
    {
        let TableFactor::Table { name, .. } = relation else {
            continue;
        };

        if is_schemaless_table(schema_map, name) {
            has_schemaless = true;
        } else {
            has_schemaful = true;
        }
    }

    if has_schemaless && has_schemaful {
        return Err(PlanError::SchemalessMixedJoinWildcardProjection.into());
    }

    Ok(())
}

fn is_schemaless_table(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    table_name: &str,
) -> bool {
    schema_map
        .get(table_name)
        .is_some_and(|schema| schema.column_defs.is_none())
}

#[cfg(test)]
mod tests {
    use {
        super::{super::plan as plan_schemaless, validate_statement},
        crate::{
            ast::{Expr, JoinExecutor, SetExpr, Statement},
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::{PlanError, fetch_schema_map},
            translate::translate,
        },
        futures::executor::block_on,
    };

    fn setup_storage() -> MockStorage {
        run("
            CREATE TABLE Player;
            CREATE TABLE Item (id INTEGER);
        ")
    }

    fn assert_mixed_join_wildcard_error(storage: &MockStorage, sql: &str) {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(storage, &statement)).unwrap();
        let planned = plan_schemaless(&schema_map, statement);

        assert_eq!(
            planned,
            Err(PlanError::SchemalessMixedJoinWildcardProjection.into()),
            "{sql}"
        );
    }

    fn assert_plan_ok(storage: &MockStorage, sql: &str) {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(storage, &statement)).unwrap();
        let planned = plan_schemaless(&schema_map, statement);
        assert!(planned.is_ok(), "{sql}");
    }

    #[test]
    fn rejects_mixed_join_wildcard_projection() {
        let storage = setup_storage();

        assert_mixed_join_wildcard_error(
            &storage,
            "SELECT * FROM Player JOIN Item WHERE Player.id = Item.id",
        );
        assert_mixed_join_wildcard_error(
            &storage,
            "SELECT * FROM Item JOIN Player WHERE Item.id = Player.id",
        );
    }

    #[test]
    fn rejects_mixed_join_wildcard_in_derived_subquery() {
        let storage = setup_storage();

        assert_mixed_join_wildcard_error(
            &storage,
            "SELECT * FROM (SELECT * FROM Player JOIN Item WHERE Player.id = Item.id) AS mixed",
        );
    }

    #[test]
    fn rejects_mixed_join_wildcard_in_expression_subqueries() {
        let storage = setup_storage();

        assert_mixed_join_wildcard_error(
            &storage,
            "SELECT id FROM Item WHERE EXISTS (SELECT * FROM Player JOIN Item WHERE Player.id = Item.id)",
        );
        assert_mixed_join_wildcard_error(
            &storage,
            "SELECT id FROM Item WHERE id IN (SELECT * FROM Player JOIN Item WHERE Player.id = Item.id)",
        );
        assert_mixed_join_wildcard_error(
            &storage,
            "SELECT (SELECT * FROM Player JOIN Item WHERE Player.id = Item.id LIMIT 1) FROM Item",
        );
    }

    #[test]
    fn rejects_mixed_join_wildcard_in_insert_create_table_update_delete() {
        let storage = setup_storage();

        assert_mixed_join_wildcard_error(
            &storage,
            "INSERT INTO Player SELECT * FROM Player JOIN Item WHERE Player.id = Item.id",
        );
        assert_mixed_join_wildcard_error(
            &storage,
            "INSERT INTO Player VALUES ((SELECT * FROM Player JOIN Item WHERE Player.id = Item.id LIMIT 1))",
        );
        assert_mixed_join_wildcard_error(
            &storage,
            "CREATE TABLE NewItem AS SELECT * FROM Player JOIN Item WHERE Player.id = Item.id",
        );
        assert_mixed_join_wildcard_error(
            &storage,
            "UPDATE Player SET id = (SELECT * FROM Player JOIN Item WHERE Player.id = Item.id LIMIT 1)",
        );
        assert_mixed_join_wildcard_error(
            &storage,
            "UPDATE Player SET id = 1 WHERE EXISTS (SELECT * FROM Player JOIN Item WHERE Player.id = Item.id)",
        );
        assert_mixed_join_wildcard_error(
            &storage,
            "DELETE FROM Player WHERE EXISTS (SELECT * FROM Player JOIN Item WHERE Player.id = Item.id)",
        );
    }

    #[test]
    fn allows_statement_when_mixed_join_wildcard_is_absent() {
        let storage = setup_storage();
        assert_plan_ok(
            &storage,
            "SELECT Item.id FROM Player JOIN Item WHERE Player.id = Item.id",
        );
    }

    #[test]
    fn validates_query_order_by_limit_offset_paths() {
        let storage = setup_storage();
        let sql = "SELECT id FROM Player ORDER BY id LIMIT 1 OFFSET 0";
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();

        assert!(validate_statement(&schema_map, &statement).is_ok(), "{sql}");
    }

    #[test]
    fn validates_values_query_path() {
        let storage = setup_storage();
        let sql = "VALUES (1), (2)";
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();

        assert!(validate_statement(&schema_map, &statement).is_ok(), "{sql}");
    }

    #[test]
    fn validates_select_clauses_and_join_on_paths() {
        let storage = setup_storage();
        assert_plan_ok(
            &storage,
            "SELECT Item.id FROM Player JOIN Item ON Player.id = Item.id WHERE Item.id > 0 GROUP BY Item.id HAVING Item.id > 0 ORDER BY Item.id LIMIT 1 OFFSET 0",
        );
    }

    #[test]
    fn validates_non_query_statement_path() {
        let storage = setup_storage();
        let parsed = parse("SELECT * FROM Player")
            .expect("SELECT * FROM Player")
            .into_iter()
            .next()
            .unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();

        let drop_parsed = parse("DROP TABLE IF EXISTS Temp")
            .expect("DROP TABLE IF EXISTS Temp")
            .into_iter()
            .next()
            .unwrap();
        let drop_statement = translate(&drop_parsed).unwrap();
        assert!(plan_schemaless(&schema_map, drop_statement).is_ok());
    }

    #[test]
    fn validates_hash_join_executor_path() {
        let storage = setup_storage();
        let sql = "SELECT Item.id FROM Player JOIN Item ON Player.id = Item.id";
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let mut statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();

        let mut applied = false;
        if let Statement::Query(query) = &mut statement
            && let SetExpr::Select(select) = &mut query.body
            && let Some(join) = select.from.joins.first_mut()
        {
            join.join_executor = JoinExecutor::Hash {
                key_expr: Expr::Identifier("id".to_owned()),
                value_expr: Expr::Identifier("id".to_owned()),
                where_clause: Some(Expr::Identifier("id".to_owned())),
            };
            applied = true;
        }
        assert!(applied, "failed to inject hash join executor");

        assert!(plan_schemaless(&schema_map, statement).is_ok());
    }

    #[test]
    fn validates_hash_join_executor_without_where_clause() {
        let storage = setup_storage();
        let sql = "SELECT Item.id FROM Player JOIN Item ON Player.id = Item.id";
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let mut statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();

        let mut applied = false;
        if let Statement::Query(query) = &mut statement
            && let SetExpr::Select(select) = &mut query.body
            && let Some(join) = select.from.joins.first_mut()
        {
            join.join_executor = JoinExecutor::Hash {
                key_expr: Expr::Identifier("id".to_owned()),
                value_expr: Expr::Identifier("id".to_owned()),
                where_clause: None,
            };
            applied = true;
        }
        assert!(applied, "failed to inject hash join executor");
        assert!(plan_schemaless(&schema_map, statement).is_ok());
    }

    #[test]
    fn validates_short_circuit_after_subquery_error() {
        let storage = setup_storage();
        assert_mixed_join_wildcard_error(
            &storage,
            "SELECT id FROM Item WHERE EXISTS (SELECT * FROM Player JOIN Item WHERE Player.id = Item.id) OR EXISTS (SELECT id FROM Item)",
        );
    }

    #[test]
    fn allows_wildcard_join_when_schema_kind_matches() {
        let storage = setup_storage();
        assert_plan_ok(
            &storage,
            "SELECT * FROM Player JOIN Player AS P2 ON Player.id = P2.id",
        );
    }

    #[test]
    fn validates_left_outer_join_on_path() {
        let storage = setup_storage();
        assert_plan_ok(
            &storage,
            "SELECT Item.id FROM Player LEFT JOIN Item ON Player.id = Item.id",
        );
    }

    #[test]
    fn allows_wildcard_join_with_non_table_root_relation() {
        let storage = setup_storage();
        assert_plan_ok(
            &storage,
            "SELECT * FROM (SELECT * FROM Player) AS P JOIN Item ON P._doc['id'] = Item.id",
        );
    }
}
