use {
    super::super::visit_mut::visit_mut_expr,
    crate::{
        ast::Literal,
        data::SCHEMALESS_DOC_COLUMN,
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan,
            FilterPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
            JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
            LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
            OffsetInputPlan, OffsetPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
            SchemaMap, SelectItemPlan, SelectOrderByPlan, SourcePlan, ValuesOrderByPlan,
            ValuesPlan,
        },
    },
    std::{collections::HashSet, mem},
};

struct QueryRewriteState {
    rewrite_unqualified_identifiers: bool,
    schemaless_aliases: HashSet<String>,
}

pub(super) fn transform_query(schema_map: &SchemaMap, query: &mut QueryPlan) {
    match query {
        QueryPlan::Project(project) => {
            transform_project(schema_map, project);
        }
        QueryPlan::Values(values) => {
            transform_values(schema_map, values);
        }
        QueryPlan::SelectOrderBy(order_by) => {
            transform_select_order_by(schema_map, order_by);
        }
        QueryPlan::ValuesOrderBy(order_by) => {
            transform_values_order_by(schema_map, order_by);
        }
        QueryPlan::Distinct(distinct) => {
            transform_distinct(schema_map, distinct);
        }
        QueryPlan::Offset(offset) => {
            transform_offset(schema_map, offset);
        }
        QueryPlan::Limit(LimitPlan { input, count }) => {
            let state = match input {
                LimitInputPlan::Project(project) => transform_project(schema_map, project),
                LimitInputPlan::Values(values) => transform_values(schema_map, values),
                LimitInputPlan::SelectOrderBy(order_by) => {
                    transform_select_order_by(schema_map, order_by)
                }
                LimitInputPlan::ValuesOrderBy(order_by) => {
                    transform_values_order_by(schema_map, order_by)
                }
                LimitInputPlan::Distinct(distinct) => transform_distinct(schema_map, distinct),
                LimitInputPlan::Offset(offset) => transform_offset(schema_map, offset),
            };
            transform_query_expr(schema_map, count, &state);
        }
    }
}

fn transform_offset(
    schema_map: &SchemaMap,
    OffsetPlan { input, count }: &mut OffsetPlan,
) -> QueryRewriteState {
    let state = match input {
        OffsetInputPlan::Project(project) => transform_project(schema_map, project),
        OffsetInputPlan::Values(values) => transform_values(schema_map, values),
        OffsetInputPlan::SelectOrderBy(order_by) => transform_select_order_by(schema_map, order_by),
        OffsetInputPlan::ValuesOrderBy(order_by) => transform_values_order_by(schema_map, order_by),
        OffsetInputPlan::Distinct(distinct) => transform_distinct(schema_map, distinct),
    };
    transform_query_expr(schema_map, count, &state);

    state
}

fn transform_distinct(
    schema_map: &SchemaMap,
    DistinctPlan { input }: &mut DistinctPlan,
) -> QueryRewriteState {
    match input {
        DistinctInputPlan::Project(project) => transform_project(schema_map, project),
        DistinctInputPlan::SelectOrderBy(order_by) => {
            transform_select_order_by(schema_map, order_by)
        }
    }
}

fn transform_select_order_by(
    schema_map: &SchemaMap,
    SelectOrderByPlan { input, exprs }: &mut SelectOrderByPlan,
) -> QueryRewriteState {
    let state = transform_project(schema_map, input);
    for order_by in exprs {
        transform_query_expr(schema_map, &mut order_by.expr, &state);
    }

    state
}

fn transform_values_order_by(
    schema_map: &SchemaMap,
    ValuesOrderByPlan { input, exprs }: &mut ValuesOrderByPlan,
) -> QueryRewriteState {
    let state = transform_values(schema_map, input);
    for order_by in exprs {
        transform_query_expr(schema_map, &mut order_by.expr, &state);
    }

    state
}

fn transform_values(
    schema_map: &SchemaMap,
    ValuesPlan(rows): &mut ValuesPlan,
) -> QueryRewriteState {
    let state = empty_rewrite_state();
    for expr in rows.iter_mut().flatten() {
        transform_query_expr(schema_map, expr, &state);
    }

    state
}

fn empty_rewrite_state() -> QueryRewriteState {
    QueryRewriteState {
        rewrite_unqualified_identifiers: false,
        schemaless_aliases: HashSet::new(),
    }
}

fn transform_project(schema_map: &SchemaMap, project: &mut ProjectPlan) -> QueryRewriteState {
    let ProjectPlan { input, projection } = project;
    let state = match input {
        ProjectInputPlan::Source(relation) => transform_source(schema_map, relation),
        ProjectInputPlan::InnerJoin(join) => transform_inner_join(schema_map, join),
        ProjectInputPlan::LeftOuterJoin(join) => transform_left_outer_join(schema_map, join),
        ProjectInputPlan::Filter(filter) => transform_filter(schema_map, filter),
        ProjectInputPlan::Aggregation(aggregation) => {
            let state = transform_aggregation_input(schema_map, &mut aggregation.input);
            for group_by in &mut aggregation.group_by {
                transform_query_expr(schema_map, group_by, &state);
            }
            state
        }
        ProjectInputPlan::Having(having) => {
            let state = transform_aggregation_input(schema_map, &mut having.input.input);
            for group_by in &mut having.input.group_by {
                transform_query_expr(schema_map, group_by, &state);
            }
            transform_query_expr(schema_map, &mut having.expr, &state);
            state
        }
    };
    let (base_source, has_join) = project_source(input);
    rewrite_projection(schema_map, projection, base_source, has_join, &state);

    state
}

fn transform_filter(
    schema_map: &SchemaMap,
    FilterPlan { input, expr }: &mut FilterPlan,
) -> QueryRewriteState {
    let state = match input {
        FilterInputPlan::Source(relation) => transform_source(schema_map, relation),
        FilterInputPlan::InnerJoin(join) => transform_inner_join(schema_map, join),
        FilterInputPlan::LeftOuterJoin(join) => transform_left_outer_join(schema_map, join),
    };
    transform_query_expr(schema_map, expr, &state);

    state
}

fn transform_aggregation_input(
    schema_map: &SchemaMap,
    input: &mut AggregationInputPlan,
) -> QueryRewriteState {
    match input {
        AggregationInputPlan::Source(relation) => transform_source(schema_map, relation),
        AggregationInputPlan::InnerJoin(join) => transform_inner_join(schema_map, join),
        AggregationInputPlan::LeftOuterJoin(join) => transform_left_outer_join(schema_map, join),
        AggregationInputPlan::Filter(filter) => transform_filter(schema_map, filter),
    }
}

fn transform_source(schema_map: &SchemaMap, relation: &mut SourcePlan) -> QueryRewriteState {
    let rewrite_unqualified_identifiers = matches!(
        relation,
        SourcePlan::Table(table) if is_schemaless_table(schema_map, &table.name)
    );
    let mut schemaless_aliases = HashSet::new();
    collect_schemaless_alias(schema_map, relation, &mut schemaless_aliases);
    let state = QueryRewriteState {
        rewrite_unqualified_identifiers,
        schemaless_aliases,
    };

    rewrite_source(schema_map, relation);
    state
}

fn transform_inner_join(schema_map: &SchemaMap, join: &mut InnerJoinPlan) -> QueryRewriteState {
    let base_source = join.base_source();
    let rewrite_unqualified_identifiers = matches!(
        base_source,
        SourcePlan::Table(table) if is_schemaless_table(schema_map, &table.name)
    );
    let mut schemaless_aliases = HashSet::new();
    collect_schemaless_alias(schema_map, base_source, &mut schemaless_aliases);
    for source in join.joined_sources() {
        collect_schemaless_alias(schema_map, source, &mut schemaless_aliases);
    }
    let state = QueryRewriteState {
        rewrite_unqualified_identifiers,
        schemaless_aliases,
    };

    rewrite_inner_join(schema_map, join, &state);
    state
}

fn transform_left_outer_join(
    schema_map: &SchemaMap,
    join: &mut LeftOuterJoinPlan,
) -> QueryRewriteState {
    let base_source = join.base_source();
    let rewrite_unqualified_identifiers = matches!(
        base_source,
        SourcePlan::Table(table) if is_schemaless_table(schema_map, &table.name)
    );
    let mut schemaless_aliases = HashSet::new();
    collect_schemaless_alias(schema_map, base_source, &mut schemaless_aliases);
    for source in join.joined_sources() {
        collect_schemaless_alias(schema_map, source, &mut schemaless_aliases);
    }
    let state = QueryRewriteState {
        rewrite_unqualified_identifiers,
        schemaless_aliases,
    };

    rewrite_left_outer_join(schema_map, join, &state);
    state
}

fn collect_schemaless_alias(
    schema_map: &SchemaMap,
    relation: &SourcePlan,
    aliases: &mut HashSet<String>,
) {
    if let SourcePlan::Table(table) = relation
        && is_schemaless_table(schema_map, &table.name)
    {
        aliases.insert(table.name.clone());
        if let Some(alias) = &table.alias {
            aliases.insert(alias.name.clone());
        }
    }
}

fn rewrite_inner_join(schema_map: &SchemaMap, join: &mut InnerJoinPlan, state: &QueryRewriteState) {
    match &mut join.input {
        InnerJoinInputPlan::NestedLoop(join) => rewrite_nested_loop(schema_map, join, state),
        InnerJoinInputPlan::Hash(join) => rewrite_hash(schema_map, join, state),
        InnerJoinInputPlan::Condition(condition) => {
            rewrite_condition(schema_map, condition, state);
        }
    }
}

fn rewrite_left_outer_join(
    schema_map: &SchemaMap,
    join: &mut LeftOuterJoinPlan,
    state: &QueryRewriteState,
) {
    match &mut join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => rewrite_nested_loop(schema_map, join, state),
        LeftOuterJoinInputPlan::Hash(join) => rewrite_hash(schema_map, join, state),
        LeftOuterJoinInputPlan::Condition(condition) => {
            rewrite_condition(schema_map, condition, state);
        }
    }
}

fn rewrite_condition(
    schema_map: &SchemaMap,
    condition: &mut JoinConditionPlan,
    state: &QueryRewriteState,
) {
    match &mut condition.input {
        JoinConditionInputPlan::NestedLoop(join) => rewrite_nested_loop(schema_map, join, state),
        JoinConditionInputPlan::Hash(join) => rewrite_hash(schema_map, join, state),
    }
    transform_query_expr(schema_map, &mut condition.expr, state);
}

fn rewrite_nested_loop(
    schema_map: &SchemaMap,
    join: &mut NestedLoopJoinPlan,
    state: &QueryRewriteState,
) {
    match &mut join.input {
        NestedLoopJoinInputPlan::Source(source) => rewrite_source(schema_map, source),
        NestedLoopJoinInputPlan::InnerJoin(join) => rewrite_inner_join(schema_map, join, state),
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
            rewrite_left_outer_join(schema_map, join, state);
        }
    }
    rewrite_source(schema_map, &mut join.right);
}

fn rewrite_hash(schema_map: &SchemaMap, join: &mut HashJoinPlan, state: &QueryRewriteState) {
    match &mut join.input {
        HashJoinInputPlan::Source(source) => rewrite_source(schema_map, source),
        HashJoinInputPlan::InnerJoin(join) => rewrite_inner_join(schema_map, join, state),
        HashJoinInputPlan::LeftOuterJoin(join) => {
            rewrite_left_outer_join(schema_map, join, state);
        }
    }
    rewrite_source(schema_map, &mut join.right);
    transform_query_expr(schema_map, &mut join.input_key, state);
    transform_query_expr(schema_map, &mut join.right_key, state);
    if let Some(right_filter) = &mut join.right_filter {
        transform_query_expr(schema_map, right_filter, state);
    }
}

fn rewrite_source(schema_map: &SchemaMap, relation: &mut SourcePlan) {
    match relation {
        SourcePlan::Derived(derived) => transform_query(schema_map, &mut derived.query),
        SourcePlan::Series(series) => {
            transform_query_expr(schema_map, &mut series.size, &empty_rewrite_state());
        }
        SourcePlan::Table(_) | SourcePlan::Dictionary(_) => {}
    }
}

fn rewrite_projection(
    schema_map: &SchemaMap,
    projection: &mut ProjectionPlan,
    base_source: &SourcePlan,
    has_join: bool,
    state: &QueryRewriteState,
) {
    let root_wildcard_maps_to_doc = state.rewrite_unqualified_identifiers && !has_join;
    let use_schemaless_map_projection = match &projection {
        ProjectionPlan::SelectItems(projection) if root_wildcard_maps_to_doc => {
            match projection.as_slice() {
                [SelectItemPlan::Wildcard] => true,
                [SelectItemPlan::QualifiedWildcard(alias)] => matches!(
                    base_source,
                    SourcePlan::Table(table)
                    if alias == &table.name
                        || table
                            .alias
                            .as_ref()
                            .is_some_and(|table_alias| table_alias.name == *alias)
                ),
                _ => false,
            }
        }
        _ => false,
    };

    if use_schemaless_map_projection {
        *projection = ProjectionPlan::SchemalessMap;
    }

    // Pass 1: rewrite identifier-based expressions.
    if let ProjectionPlan::SelectItems(items) = projection {
        for item in items.iter_mut() {
            if let SelectItemPlan::Expr { expr, .. } = item {
                transform_query_expr(schema_map, expr, state);
            }
        }
        // Pass 2: rewrite wildcard projections.
        for item in items {
            transform_wildcard_projection(
                item,
                root_wildcard_maps_to_doc,
                &state.schemaless_aliases,
            );
        }
    }
}

fn project_source(input: &ProjectInputPlan) -> (&SourcePlan, bool) {
    (input.base_source(), !input.joined_sources().is_empty())
}

fn transform_query_expr(schema_map: &SchemaMap, expr: &mut ExprPlan, state: &QueryRewriteState) {
    visit_mut_expr(expr, &mut |e| match e {
        ExprPlan::Identifier(ident) => {
            if state.rewrite_unqualified_identifiers {
                *e = ExprPlan::ArrayIndex {
                    obj: Box::new(ExprPlan::Identifier(SCHEMALESS_DOC_COLUMN.to_owned())),
                    indexes: vec![ExprPlan::Literal(Literal::QuotedString(ident.to_owned()))],
                };
            }
        }
        ExprPlan::CompoundIdentifier { alias, ident } => {
            if state.schemaless_aliases.contains(alias) {
                *e = ExprPlan::ArrayIndex {
                    obj: Box::new(ExprPlan::CompoundIdentifier {
                        alias: alias.to_owned(),
                        ident: SCHEMALESS_DOC_COLUMN.to_owned(),
                    }),
                    indexes: vec![ExprPlan::Literal(Literal::QuotedString(ident.to_owned()))],
                };
            }
        }
        ExprPlan::Subquery(subquery)
        | ExprPlan::Exists { subquery, .. }
        | ExprPlan::InSubquery { subquery, .. } => {
            transform_query(schema_map, subquery.as_mut());
        }
        _ => {}
    });
}

fn transform_wildcard_projection(
    item: &mut SelectItemPlan,
    root_wildcard_maps_to_doc: bool,
    schemaless_aliases: &HashSet<String>,
) {
    match item {
        SelectItemPlan::Expr { .. } => {}
        SelectItemPlan::Wildcard => {
            if root_wildcard_maps_to_doc {
                *item = SelectItemPlan::Expr {
                    expr: ExprPlan::Identifier(SCHEMALESS_DOC_COLUMN.to_owned()),
                    label: SCHEMALESS_DOC_COLUMN.to_owned(),
                };
            }
        }
        SelectItemPlan::QualifiedWildcard(alias) => {
            if schemaless_aliases.contains(alias) {
                let alias = mem::take(alias);
                *item = SelectItemPlan::Expr {
                    expr: ExprPlan::CompoundIdentifier {
                        alias,
                        ident: SCHEMALESS_DOC_COLUMN.to_owned(),
                    },
                    label: SCHEMALESS_DOC_COLUMN.to_owned(),
                };
            }
        }
    }
}

fn is_schemaless_table(schema_map: &SchemaMap, table_name: &str) -> bool {
    schema_map
        .get(table_name)
        .is_some_and(|schema| schema.column_defs.is_none())
}

#[cfg(test)]
mod tests {
    use {
        super::transform_query,
        crate::{
            mock::run,
            parse_sql::parse_query,
            plan::{QueryPlan, SchemaMap},
            store::Store,
            translate::{NO_PARAMS, translate_query},
        },
        pretty_assertions::assert_eq,
    };

    fn query(sql: &str) -> QueryPlan {
        translate_query(&parse_query(sql).expect(sql), NO_PARAMS)
            .expect(sql)
            .into()
    }

    #[test]
    fn values() {
        let storage = run("CREATE TABLE Doc");
        let schema = storage.fetch_schema("Doc").unwrap().unwrap();
        let schema_map = SchemaMap::from([("Doc".to_owned(), schema.into())]);
        let test = |actual: &str, expected: &str, name: &str| {
            let mut result = query(actual);
            transform_query(&schema_map, &mut result);
            let expected_plan = query(expected);

            assert_eq!(
                result, expected_plan,
                "\n[{name}]\nactual: {actual}\nexpected: {expected}"
            );
        };

        let actual = "
            VALUES
                ((SELECT x FROM Doc), outer_value),
                ((SELECT d.x FROM Doc d), 2)
        ";
        let expected = "
            VALUES
                ((SELECT _doc['x'] AS x FROM Doc), outer_value),
                ((SELECT d._doc['x'] AS x FROM Doc d), 2)
        ";
        test(actual, expected, "multiple rows");

        let actual = "
            VALUES
                ((SELECT x FROM Doc), outer_value),
                ((SELECT d.x FROM Doc d), 2)
            ORDER BY column1
        ";
        let expected = "
            VALUES
                ((SELECT _doc['x'] AS x FROM Doc), outer_value),
                ((SELECT d._doc['x'] AS x FROM Doc d), 2)
            ORDER BY column1
        ";
        test(actual, expected, "order by");

        let actual = "
            VALUES
                ((SELECT x FROM Doc), outer_value),
                ((SELECT d.x FROM Doc d), 2)
            LIMIT 1
        ";
        let expected = "
            VALUES
                ((SELECT _doc['x'] AS x FROM Doc), outer_value),
                ((SELECT d._doc['x'] AS x FROM Doc d), 2)
            LIMIT 1
        ";
        test(actual, expected, "limit");

        let actual = "
            VALUES
                ((SELECT x FROM Doc), outer_value),
                ((SELECT d.x FROM Doc d), 2)
            OFFSET 1
        ";
        let expected = "
            VALUES
                ((SELECT _doc['x'] AS x FROM Doc), outer_value),
                ((SELECT d._doc['x'] AS x FROM Doc d), 2)
            OFFSET 1
        ";
        test(actual, expected, "offset");

        let actual = "
            VALUES
                ((SELECT x FROM Doc), outer_value),
                ((SELECT d.x FROM Doc d), 2)
            ORDER BY column1
            LIMIT 1
            OFFSET 1
        ";
        let expected = "
            VALUES
                ((SELECT _doc['x'] AS x FROM Doc), outer_value),
                ((SELECT d._doc['x'] AS x FROM Doc d), 2)
            ORDER BY column1
            LIMIT 1
            OFFSET 1
        ";
        test(actual, expected, "order by with limit and offset");
    }

    #[test]
    fn series() {
        let storage = run("CREATE TABLE Doc");
        let schema = storage.fetch_schema("Doc").unwrap().unwrap();
        let schema_map = SchemaMap::from([("Doc".to_owned(), schema.into())]);
        let test = |actual: &str, expected: &str, name: &str| {
            let mut result = query(actual);
            transform_query(&schema_map, &mut result);
            let expected_plan = query(expected);

            assert_eq!(
                result, expected_plan,
                "\n[{name}]\nactual: {actual}\nexpected: {expected}"
            );
        };

        let actual = "
            SELECT *
            FROM SERIES(outer_size + (
                SELECT d.x FROM Doc d
            ))
        ";
        let expected = "
            SELECT *
            FROM SERIES(outer_size + (
                SELECT d._doc['x'] AS x FROM Doc d
            ))
        ";
        test(actual, expected, "size subquery with outer identifier");
    }
}
