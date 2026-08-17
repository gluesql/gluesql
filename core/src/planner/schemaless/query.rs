use {
    crate::{
        ast::Literal,
        data::{SCHEMALESS_DOC_COLUMN, Schema},
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan,
            FilterPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
            JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
            LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
            OffsetInputPlan, OffsetPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
            SelectItemPlan, SelectOrderByPlan, SourcePlan, ValuesOrderByPlan, visit_mut_expr,
        },
    },
    std::{
        collections::{HashMap, HashSet},
        hash::BuildHasher,
        mem,
    },
};

struct QueryRewriteState {
    unqualified_schemaless_alias: Option<String>,
    schemaless_aliases: HashSet<String>,
}

pub(super) fn transform_query<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    query: &mut QueryPlan,
) {
    match query {
        QueryPlan::Project(project) => {
            transform_project(schema_map, project);
        }
        QueryPlan::Values(_) => {}
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
                LimitInputPlan::Values(_) => empty_rewrite_state(),
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

fn transform_offset<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    OffsetPlan { input, count }: &mut OffsetPlan,
) -> QueryRewriteState {
    let state = match input {
        OffsetInputPlan::Project(project) => transform_project(schema_map, project),
        OffsetInputPlan::Values(_) => empty_rewrite_state(),
        OffsetInputPlan::SelectOrderBy(order_by) => transform_select_order_by(schema_map, order_by),
        OffsetInputPlan::ValuesOrderBy(order_by) => transform_values_order_by(schema_map, order_by),
        OffsetInputPlan::Distinct(distinct) => transform_distinct(schema_map, distinct),
    };
    transform_query_expr(schema_map, count, &state);

    state
}

fn transform_distinct<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    DistinctPlan { input }: &mut DistinctPlan,
) -> QueryRewriteState {
    match input {
        DistinctInputPlan::Project(project) => transform_project(schema_map, project),
        DistinctInputPlan::SelectOrderBy(order_by) => {
            transform_select_order_by(schema_map, order_by)
        }
    }
}

fn transform_select_order_by<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    SelectOrderByPlan { input, exprs }: &mut SelectOrderByPlan,
) -> QueryRewriteState {
    let state = transform_project(schema_map, input);
    for order_by in exprs {
        transform_query_expr(schema_map, &mut order_by.expr, &state);
    }

    state
}

fn transform_values_order_by<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    ValuesOrderByPlan { exprs, .. }: &mut ValuesOrderByPlan,
) -> QueryRewriteState {
    let state = empty_rewrite_state();
    for order_by in exprs {
        transform_query_expr(schema_map, &mut order_by.expr, &state);
    }

    state
}

fn empty_rewrite_state() -> QueryRewriteState {
    QueryRewriteState {
        unqualified_schemaless_alias: None,
        schemaless_aliases: HashSet::new(),
    }
}

fn transform_project<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    project: &mut ProjectPlan,
) -> QueryRewriteState {
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

fn transform_filter<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
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

fn transform_aggregation_input<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut AggregationInputPlan,
) -> QueryRewriteState {
    match input {
        AggregationInputPlan::Source(relation) => transform_source(schema_map, relation),
        AggregationInputPlan::InnerJoin(join) => transform_inner_join(schema_map, join),
        AggregationInputPlan::LeftOuterJoin(join) => transform_left_outer_join(schema_map, join),
        AggregationInputPlan::Filter(filter) => transform_filter(schema_map, filter),
    }
}

fn transform_source<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    relation: &mut SourcePlan,
) -> QueryRewriteState {
    let unqualified_schemaless_alias = root_schemaless_alias(schema_map, relation);
    let mut schemaless_aliases = HashSet::new();
    collect_schemaless_alias(schema_map, relation, &mut schemaless_aliases);
    let state = QueryRewriteState {
        unqualified_schemaless_alias,
        schemaless_aliases,
    };

    rewrite_source(schema_map, relation);
    state
}

fn transform_inner_join<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut InnerJoinPlan,
) -> QueryRewriteState {
    let base_source = join.base_source();
    let joined_sources = join.joined_sources();
    let state = join_rewrite_state(schema_map, base_source, &joined_sources);

    rewrite_inner_join(schema_map, join);
    state
}

fn transform_left_outer_join<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut LeftOuterJoinPlan,
) -> QueryRewriteState {
    let base_source = join.base_source();
    let joined_sources = join.joined_sources();
    let state = join_rewrite_state(schema_map, base_source, &joined_sources);

    rewrite_left_outer_join(schema_map, join);
    state
}

fn join_rewrite_state(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    base_source: &SourcePlan,
    joined_sources: &[&SourcePlan],
) -> QueryRewriteState {
    let unqualified_schemaless_alias = joined_sources
        .iter()
        .rev()
        .find_map(|source| root_schemaless_alias(schema_map, source))
        .or_else(|| root_schemaless_alias(schema_map, base_source));
    let mut schemaless_aliases = HashSet::new();
    collect_schemaless_alias(schema_map, base_source, &mut schemaless_aliases);
    for source in joined_sources {
        collect_schemaless_alias(schema_map, source, &mut schemaless_aliases);
    }
    QueryRewriteState {
        unqualified_schemaless_alias,
        schemaless_aliases,
    }
}

fn collect_schemaless_alias(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
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

fn root_schemaless_alias(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    relation: &SourcePlan,
) -> Option<String> {
    match relation {
        SourcePlan::Table(table) if is_schemaless_table(schema_map, &table.name) => Some(
            table
                .alias
                .as_ref()
                .map_or_else(|| table.name.clone(), |alias| alias.name.clone()),
        ),
        _ => None,
    }
}

fn rewrite_inner_join(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    join: &mut InnerJoinPlan,
) {
    match &mut join.input {
        InnerJoinInputPlan::NestedLoop(join) => rewrite_nested_loop(schema_map, join),
        InnerJoinInputPlan::Hash(join) => rewrite_hash(schema_map, join),
        InnerJoinInputPlan::Condition(condition) => {
            rewrite_condition(schema_map, condition);
        }
    }
}

fn rewrite_left_outer_join(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    join: &mut LeftOuterJoinPlan,
) {
    match &mut join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => rewrite_nested_loop(schema_map, join),
        LeftOuterJoinInputPlan::Hash(join) => rewrite_hash(schema_map, join),
        LeftOuterJoinInputPlan::Condition(condition) => {
            rewrite_condition(schema_map, condition);
        }
    }
}

fn rewrite_condition(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    condition: &mut JoinConditionPlan,
) {
    let joined_sources = condition.joined_sources();
    let state = join_rewrite_state(schema_map, condition.base_source(), &joined_sources);

    match &mut condition.input {
        JoinConditionInputPlan::NestedLoop(join) => rewrite_nested_loop(schema_map, join),
        JoinConditionInputPlan::Hash(join) => rewrite_hash(schema_map, join),
    }
    transform_query_expr(schema_map, &mut condition.expr, &state);
}

fn rewrite_nested_loop(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    join: &mut NestedLoopJoinPlan,
) {
    match &mut join.input {
        NestedLoopJoinInputPlan::Source(source) => rewrite_source(schema_map, source),
        NestedLoopJoinInputPlan::InnerJoin(join) => rewrite_inner_join(schema_map, join),
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
            rewrite_left_outer_join(schema_map, join);
        }
    }
    rewrite_source(schema_map, &mut join.right);
}

fn rewrite_hash(schema_map: &HashMap<String, Schema, impl BuildHasher>, join: &mut HashJoinPlan) {
    let joined_sources = join.joined_sources();
    let state = join_rewrite_state(schema_map, join.base_source(), &joined_sources);

    match &mut join.input {
        HashJoinInputPlan::Source(source) => rewrite_source(schema_map, source),
        HashJoinInputPlan::InnerJoin(join) => rewrite_inner_join(schema_map, join),
        HashJoinInputPlan::LeftOuterJoin(join) => {
            rewrite_left_outer_join(schema_map, join);
        }
    }
    rewrite_source(schema_map, &mut join.right);
    transform_query_expr(schema_map, &mut join.input_key, &state);
    transform_query_expr(schema_map, &mut join.right_key, &state);
    if let Some(right_filter) = &mut join.right_filter {
        transform_query_expr(schema_map, right_filter, &state);
    }
}

fn rewrite_source(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    relation: &mut SourcePlan,
) {
    if let SourcePlan::Derived(derived) = relation {
        transform_query(schema_map, &mut derived.query);
    }
}

fn rewrite_projection(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    projection: &mut ProjectionPlan,
    base_source: &SourcePlan,
    has_join: bool,
    state: &QueryRewriteState,
) {
    let root_wildcard_maps_to_doc = state.unqualified_schemaless_alias.is_some() && !has_join;
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
                if root_wildcard_maps_to_doc {
                    state.unqualified_schemaless_alias.as_deref()
                } else {
                    None
                },
                &state.schemaless_aliases,
            );
        }
    }
}

fn project_source(input: &ProjectInputPlan) -> (&SourcePlan, bool) {
    (input.base_source(), !input.joined_sources().is_empty())
}

fn transform_query_expr(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    expr: &mut ExprPlan,
    state: &QueryRewriteState,
) {
    visit_mut_expr(expr, &mut |e| match e {
        ExprPlan::UnplannedReference {
            qualifier: None,
            name: ident,
        } => {
            if let Some(alias) = &state.unqualified_schemaless_alias
                && ident != SCHEMALESS_DOC_COLUMN
            {
                *e = ExprPlan::ArrayIndex {
                    obj: Box::new(ExprPlan::ResolvedColumn {
                        alias: alias.clone(),
                        column: SCHEMALESS_DOC_COLUMN.to_owned(),
                    }),
                    indexes: vec![ExprPlan::Literal(Literal::QuotedString(ident.to_owned()))],
                };
            }
        }
        ExprPlan::UnplannedReference {
            qualifier: Some(alias),
            name: ident,
        } => {
            if state.schemaless_aliases.contains(alias) && ident != SCHEMALESS_DOC_COLUMN {
                *e = ExprPlan::ArrayIndex {
                    obj: Box::new(ExprPlan::ResolvedColumn {
                        alias: alias.to_owned(),
                        column: SCHEMALESS_DOC_COLUMN.to_owned(),
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
    root_schemaless_alias: Option<&str>,
    schemaless_aliases: &HashSet<String>,
) {
    match item {
        SelectItemPlan::Expr { .. } => {}
        SelectItemPlan::Wildcard => {
            if let Some(alias) = root_schemaless_alias {
                *item = SelectItemPlan::Expr {
                    expr: ExprPlan::ResolvedColumn {
                        alias: alias.to_owned(),
                        column: SCHEMALESS_DOC_COLUMN.to_owned(),
                    },
                    label: SCHEMALESS_DOC_COLUMN.to_owned(),
                };
            }
        }
        SelectItemPlan::QualifiedWildcard(alias) => {
            if schemaless_aliases.contains(alias) {
                let alias = mem::take(alias);
                *item = SelectItemPlan::Expr {
                    expr: ExprPlan::ResolvedColumn {
                        alias,
                        column: SCHEMALESS_DOC_COLUMN.to_owned(),
                    },
                    label: SCHEMALESS_DOC_COLUMN.to_owned(),
                };
            }
        }
    }
}

fn is_schemaless_table(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    table_name: &str,
) -> bool {
    schema_map
        .get(table_name)
        .is_some_and(|schema| schema.column_defs.is_none())
}
