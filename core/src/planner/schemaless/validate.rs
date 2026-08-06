use {
    super::super::{PlannerError, expr::try_visit_expr},
    crate::{
        data::Schema,
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan,
            FilterPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
            JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
            LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
            OffsetInputPlan, OffsetPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
            SelectItemPlan, SelectOrderByPlan, SourcePlan, StatementPlan, ValuesOrderByPlan,
            ValuesPlan,
        },
        result::Result,
    },
    std::{collections::HashMap, hash::BuildHasher},
};

type ValidateResult = std::result::Result<(), PlannerError>;

/// Rejects schemaless-specific unsupported patterns before rewrite.
pub(super) fn validate_statement<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: &StatementPlan,
) -> Result<()> {
    validate_statement_inner(schema_map, statement).map_err(Into::into)
}

fn validate_statement_inner(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    statement: &StatementPlan,
) -> ValidateResult {
    match statement {
        StatementPlan::Query(query) => validate_query(schema_map, query),
        StatementPlan::Insert {
            table_name,
            columns,
            source,
        } => {
            if !columns.is_empty() && is_schemaless_table(schema_map, table_name) {
                return Err(PlannerError::SchemalessInsertWithExplicitColumns);
            }

            validate_query(schema_map, source)
        }
        StatementPlan::CreateTable { source, .. } => source
            .as_ref()
            .map_or(Ok(()), |query| validate_query(schema_map, query)),
        StatementPlan::Update {
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
        StatementPlan::Delete { selection, .. } => selection
            .as_ref()
            .map_or(Ok(()), |expr| validate_expr(schema_map, expr)),
        _ => Ok(()),
    }
}

fn validate_query(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    query: &QueryPlan,
) -> ValidateResult {
    match query {
        QueryPlan::Project(project) => validate_project(schema_map, project),
        QueryPlan::Values(values) => validate_values(schema_map, values),
        QueryPlan::SelectOrderBy(order_by) => validate_select_order_by(schema_map, order_by),
        QueryPlan::ValuesOrderBy(order_by) => validate_values_order_by(schema_map, order_by),
        QueryPlan::Distinct(distinct) => validate_distinct(schema_map, distinct),
        QueryPlan::Offset(offset) => validate_offset(schema_map, offset),
        QueryPlan::Limit(LimitPlan { input, count }) => {
            match input {
                LimitInputPlan::Project(project) => validate_project(schema_map, project)?,
                LimitInputPlan::Values(values) => validate_values(schema_map, values)?,
                LimitInputPlan::SelectOrderBy(order_by) => {
                    validate_select_order_by(schema_map, order_by)?;
                }
                LimitInputPlan::ValuesOrderBy(order_by) => {
                    validate_values_order_by(schema_map, order_by)?;
                }
                LimitInputPlan::Distinct(distinct) => validate_distinct(schema_map, distinct)?,
                LimitInputPlan::Offset(offset) => validate_offset(schema_map, offset)?,
            }

            validate_expr(schema_map, count)
        }
    }
}

fn validate_offset(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    OffsetPlan { input, count }: &OffsetPlan,
) -> ValidateResult {
    match input {
        OffsetInputPlan::Project(project) => validate_project(schema_map, project)?,
        OffsetInputPlan::Values(values) => validate_values(schema_map, values)?,
        OffsetInputPlan::SelectOrderBy(order_by) => {
            validate_select_order_by(schema_map, order_by)?;
        }
        OffsetInputPlan::ValuesOrderBy(order_by) => {
            validate_values_order_by(schema_map, order_by)?;
        }
        OffsetInputPlan::Distinct(distinct) => validate_distinct(schema_map, distinct)?,
    }

    validate_expr(schema_map, count)
}

fn validate_distinct(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    DistinctPlan { input }: &DistinctPlan,
) -> ValidateResult {
    match input {
        DistinctInputPlan::Project(project) => validate_project(schema_map, project),
        DistinctInputPlan::SelectOrderBy(order_by) => {
            validate_select_order_by(schema_map, order_by)
        }
    }
}

fn validate_select_order_by(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    SelectOrderByPlan { input, exprs }: &SelectOrderByPlan,
) -> ValidateResult {
    validate_project(schema_map, input)?;
    for order_by in exprs {
        validate_expr(schema_map, &order_by.expr)?;
    }

    Ok(())
}

fn validate_values_order_by(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    ValuesOrderByPlan { input, exprs }: &ValuesOrderByPlan,
) -> ValidateResult {
    validate_values(schema_map, input)?;
    for order_by in exprs {
        validate_expr(schema_map, &order_by.expr)?;
    }

    Ok(())
}

fn validate_values(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    values: &ValuesPlan,
) -> ValidateResult {
    for row in &values.0 {
        for expr in row {
            validate_expr(schema_map, expr)?;
        }
    }

    Ok(())
}

fn validate_project(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    project: &ProjectPlan,
) -> ValidateResult {
    validate_mixed_join_wildcard_projection(schema_map, project)?;
    match &project.input {
        ProjectInputPlan::Source(relation) => validate_source(schema_map, relation)?,
        ProjectInputPlan::InnerJoin(join) => validate_inner_join(schema_map, join)?,
        ProjectInputPlan::LeftOuterJoin(join) => validate_left_outer_join(schema_map, join)?,
        ProjectInputPlan::Filter(filter) => validate_filter(schema_map, filter)?,
        ProjectInputPlan::Aggregation(aggregation) => {
            validate_aggregation_input(schema_map, &aggregation.input)?;
            for group_by in &aggregation.group_by {
                validate_expr(schema_map, group_by)?;
            }
        }
        ProjectInputPlan::Having(having) => {
            validate_aggregation_input(schema_map, &having.input.input)?;
            for group_by in &having.input.group_by {
                validate_expr(schema_map, group_by)?;
            }
            validate_expr(schema_map, &having.expr)?;
        }
    }

    if let ProjectionPlan::SelectItems(projection) = &project.projection {
        for projection in projection {
            if let SelectItemPlan::Expr { expr, .. } = projection {
                validate_expr(schema_map, expr)?;
            }
        }
    }

    Ok(())
}

fn validate_filter(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    FilterPlan { input, expr }: &FilterPlan,
) -> ValidateResult {
    match input {
        FilterInputPlan::Source(relation) => validate_source(schema_map, relation)?,
        FilterInputPlan::InnerJoin(join) => validate_inner_join(schema_map, join)?,
        FilterInputPlan::LeftOuterJoin(join) => validate_left_outer_join(schema_map, join)?,
    }
    validate_expr(schema_map, expr)
}

fn validate_aggregation_input(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    input: &AggregationInputPlan,
) -> ValidateResult {
    match input {
        AggregationInputPlan::Source(relation) => validate_source(schema_map, relation),
        AggregationInputPlan::InnerJoin(join) => validate_inner_join(schema_map, join),
        AggregationInputPlan::LeftOuterJoin(join) => validate_left_outer_join(schema_map, join),
        AggregationInputPlan::Filter(filter) => validate_filter(schema_map, filter),
    }
}

fn validate_inner_join(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    join: &InnerJoinPlan,
) -> ValidateResult {
    match &join.input {
        InnerJoinInputPlan::NestedLoop(join) => validate_nested_loop(schema_map, join),
        InnerJoinInputPlan::Hash(join) => validate_hash(schema_map, join),
        InnerJoinInputPlan::Condition(condition) => validate_condition(schema_map, condition),
    }
}

fn validate_left_outer_join(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    join: &LeftOuterJoinPlan,
) -> ValidateResult {
    match &join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => validate_nested_loop(schema_map, join),
        LeftOuterJoinInputPlan::Hash(join) => validate_hash(schema_map, join),
        LeftOuterJoinInputPlan::Condition(condition) => validate_condition(schema_map, condition),
    }
}

fn validate_condition(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    condition: &JoinConditionPlan,
) -> ValidateResult {
    match &condition.input {
        JoinConditionInputPlan::NestedLoop(join) => validate_nested_loop(schema_map, join)?,
        JoinConditionInputPlan::Hash(join) => validate_hash(schema_map, join)?,
    }
    validate_expr(schema_map, &condition.expr)
}

fn validate_nested_loop(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    join: &NestedLoopJoinPlan,
) -> ValidateResult {
    match &join.input {
        NestedLoopJoinInputPlan::Source(source) => validate_source(schema_map, source)?,
        NestedLoopJoinInputPlan::InnerJoin(join) => validate_inner_join(schema_map, join)?,
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
            validate_left_outer_join(schema_map, join)?;
        }
    }
    validate_source(schema_map, &join.right)
}

fn validate_hash(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    join: &HashJoinPlan,
) -> ValidateResult {
    match &join.input {
        HashJoinInputPlan::Source(source) => validate_source(schema_map, source)?,
        HashJoinInputPlan::InnerJoin(join) => validate_inner_join(schema_map, join)?,
        HashJoinInputPlan::LeftOuterJoin(join) => {
            validate_left_outer_join(schema_map, join)?;
        }
    }
    validate_source(schema_map, &join.right)?;
    validate_expr(schema_map, &join.input_key)?;
    validate_expr(schema_map, &join.right_key)?;
    if let Some(expr) = &join.right_filter {
        validate_expr(schema_map, expr)?;
    }

    Ok(())
}

fn validate_source(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    source: &SourcePlan,
) -> ValidateResult {
    match source {
        SourcePlan::Derived(derived) => validate_query(schema_map, &derived.query),
        _ => Ok(()),
    }
}

fn validate_expr(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    expr: &ExprPlan,
) -> ValidateResult {
    try_visit_expr(expr, &mut |expr| match expr {
        ExprPlan::Subquery(subquery)
        | ExprPlan::Exists { subquery, .. }
        | ExprPlan::InSubquery { subquery, .. } => validate_query(schema_map, subquery),
        _ => Ok(()),
    })
}

fn validate_mixed_join_wildcard_projection(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    project: &ProjectPlan,
) -> ValidateResult {
    if !matches!(
        &project.projection,
        ProjectionPlan::SelectItems(projection)
            if projection
                .iter()
                .any(|item| matches!(item, SelectItemPlan::Wildcard))
    ) {
        return Ok(());
    }

    let mut has_schemaless = false;
    let mut has_schemaful = false;
    let joined_sources = project.input.joined_sources();
    if joined_sources.is_empty() {
        return Ok(());
    }
    let mut classify = |source: &SourcePlan| {
        let SourcePlan::Table(table) = source else {
            return;
        };

        if is_schemaless_table(schema_map, &table.name) {
            has_schemaless = true;
        } else {
            has_schemaful = true;
        }
    };
    classify(project.input.base_source());
    for source in joined_sources {
        classify(source);
    }

    if has_schemaless && has_schemaful {
        return Err(PlannerError::SchemalessMixedJoinWildcardProjection);
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
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::StatementPlan,
            planner::{PlannerError, fetch_schema_map},
            query_builder::{Build, table},
            translate::translate,
        },
    };

    fn setup_storage() -> MockStorage {
        run("
            CREATE TABLE Player;
            CREATE TABLE Item (id INTEGER);
        ")
    }

    fn assert_mixed_join_wildcard_error(storage: &MockStorage, sql: &str) {
        assert_planner_error(
            storage,
            sql,
            PlannerError::SchemalessMixedJoinWildcardProjection,
        );
    }

    fn assert_planner_error(storage: &MockStorage, sql: &str, expected: PlannerError) {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        let schema_map = fetch_schema_map(storage, &statement).unwrap();
        let planned = plan_schemaless(&schema_map, statement);

        assert_eq!(planned, Err(expected.into()), "{sql}");
    }

    fn assert_plan_ok(storage: &MockStorage, sql: &str) {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        let schema_map = fetch_schema_map(storage, &statement).unwrap();
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
    fn rejects_schemaless_insert_with_explicit_columns() {
        let storage = setup_storage();

        assert_planner_error(
            &storage,
            "INSERT INTO Player (id, name) VALUES (1, 'Alice')",
            PlannerError::SchemalessInsertWithExplicitColumns,
        );
        assert_planner_error(
            &storage,
            "INSERT INTO Player (id) SELECT id FROM Item",
            PlannerError::SchemalessInsertWithExplicitColumns,
        );
    }

    #[test]
    fn allows_insert_with_explicit_columns_for_schemaful_table() {
        let storage = setup_storage();
        assert_plan_ok(&storage, "INSERT INTO Item (id) VALUES (1)");
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
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();

        assert!(validate_statement(&schema_map, &statement).is_ok(), "{sql}");

        assert_plan_ok(&storage, "SELECT id FROM Player ORDER BY id LIMIT 1");
    }

    #[test]
    fn validates_values_query_path() {
        let storage = setup_storage();
        let sql = "VALUES (1), (2)";
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();

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
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();

        let drop_parsed = parse("DROP TABLE IF EXISTS Temp")
            .expect("DROP TABLE IF EXISTS Temp")
            .into_iter()
            .next()
            .unwrap();
        let drop_statement = StatementPlan::from(translate(&drop_parsed).unwrap());
        assert!(plan_schemaless(&schema_map, drop_statement).is_ok());
    }

    #[test]
    fn validates_hash_join_executor_path() {
        let storage = setup_storage();
        let statement = table("Player")
            .select()
            .join("Item")
            .hash_executor("Item.id", "Player.id")
            .hash_filter("Item.id > 0")
            .project("Item.id")
            .build()
            .unwrap();
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();

        assert!(plan_schemaless(&schema_map, statement).is_ok());
    }

    #[test]
    fn validates_hash_join_executor_without_where_clause() {
        let storage = setup_storage();
        let statement = table("Player")
            .select()
            .join("Item")
            .hash_executor("Item.id", "Player.id")
            .project("Item.id")
            .build()
            .unwrap();
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();

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
