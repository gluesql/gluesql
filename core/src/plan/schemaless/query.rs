use {
    crate::{
        ast::Literal,
        data::{SCHEMALESS_DOC_COLUMN, Schema},
        plan::{
            DistinctInputPlan, DistinctPlan, ExprPlan, JoinConstraintPlan, JoinExecutorPlan,
            JoinOperatorPlan, LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan,
            ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan,
            SelectOrderByPlan, SelectPlan, TableFactorPlan, TableWithJoinsPlan, ValuesOrderByPlan,
            expr::visit_mut_expr,
        },
    },
    std::{
        collections::{HashMap, HashSet},
        hash::BuildHasher,
        iter::once,
    },
};

struct QueryRewriteState {
    rewrite_unqualified_identifiers: bool,
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
        rewrite_unqualified_identifiers: false,
        schemaless_aliases: HashSet::new(),
    }
}

fn transform_project<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    project: &mut ProjectPlan,
) -> QueryRewriteState {
    let ProjectPlan { input, projection } = project;
    let state = match input {
        ProjectInputPlan::Select(select) => transform_select(schema_map, select),
        ProjectInputPlan::Aggregation(aggregation) => {
            let state = transform_select(schema_map, &mut aggregation.input);
            for group_by in &mut aggregation.group_by {
                transform_query_expr(schema_map, group_by, &state);
            }
            state
        }
        ProjectInputPlan::Having(having) => {
            let state = transform_select(schema_map, &mut having.input.input);
            for group_by in &mut having.input.group_by {
                transform_query_expr(schema_map, group_by, &state);
            }
            transform_query_expr(schema_map, &mut having.expr, &state);
            state
        }
    };
    let select = match &*input {
        ProjectInputPlan::Select(select) => select.as_ref(),
        ProjectInputPlan::Aggregation(aggregation) => aggregation.input.as_ref(),
        ProjectInputPlan::Having(having) => having.input.input.as_ref(),
    };
    rewrite_projection(schema_map, projection, select, &state);

    state
}

fn transform_select<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    select: &mut SelectPlan,
) -> QueryRewriteState {
    let rewrite_unqualified_identifiers = matches!(
        &select.from.relation,
        TableFactorPlan::Table { name, .. } if is_schemaless_table(schema_map, name)
    );
    let schemaless_aliases = collect_schemaless_aliases(schema_map, &select.from);
    let state = QueryRewriteState {
        rewrite_unqualified_identifiers,
        schemaless_aliases,
    };

    rewrite_select(schema_map, select, &state);
    state
}

fn collect_schemaless_aliases(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    table_with_joins: &TableWithJoinsPlan,
) -> HashSet<String> {
    let TableWithJoinsPlan { relation, joins } = table_with_joins;

    let mut schemaless_aliases = HashSet::new();
    for relation in once(relation).chain(joins.iter().map(|join| &join.relation)) {
        if let TableFactorPlan::Table { name, alias, .. } = relation
            && is_schemaless_table(schema_map, name)
        {
            schemaless_aliases.insert(name.clone());
            if let Some(alias) = alias {
                schemaless_aliases.insert(alias.name.clone());
            }
        }
    }

    schemaless_aliases
}

fn rewrite_select(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    select: &mut SelectPlan,
    state: &QueryRewriteState,
) {
    for relation in once(&mut select.from.relation)
        .chain(select.from.joins.iter_mut().map(|join| &mut join.relation))
    {
        if let TableFactorPlan::Derived { subquery, .. } = relation {
            transform_query(schema_map, subquery);
        }
    }

    for join in &mut select.from.joins {
        match &mut join.join_operator {
            JoinOperatorPlan::Inner(JoinConstraintPlan::On(expr))
            | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::On(expr)) => {
                transform_query_expr(schema_map, expr, state);
            }
            _ => {}
        }

        match &mut join.join_executor {
            JoinExecutorPlan::Hash {
                key_expr,
                value_expr,
                where_clause,
            } => {
                transform_query_expr(schema_map, key_expr, state);
                transform_query_expr(schema_map, value_expr, state);
                if let Some(where_clause) = where_clause.as_mut() {
                    transform_query_expr(schema_map, where_clause, state);
                }
            }
            JoinExecutorPlan::NestedLoop => {}
        }
    }

    if let Some(selection) = select.selection.as_mut() {
        transform_query_expr(schema_map, selection, state);
    }
}

fn rewrite_projection(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    projection: &mut ProjectionPlan,
    select: &SelectPlan,
    state: &QueryRewriteState,
) {
    let root_wildcard_maps_to_doc =
        state.rewrite_unqualified_identifiers && select.from.joins.is_empty();
    let use_schemaless_map_projection = match &projection {
        ProjectionPlan::SelectItems(projection) if root_wildcard_maps_to_doc => {
            match projection.as_slice() {
                [SelectItemPlan::Wildcard] => true,
                [SelectItemPlan::QualifiedWildcard(alias)] => matches!(
                    &select.from.relation,
                    TableFactorPlan::Table {
                        name,
                        alias: table_alias,
                        ..
                    } if alias == name
                        || table_alias
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

fn transform_query_expr(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    expr: &mut ExprPlan,
    state: &QueryRewriteState,
) {
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
                let alias = std::mem::take(alias);
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

fn is_schemaless_table(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    table_name: &str,
) -> bool {
    schema_map
        .get(table_name)
        .is_some_and(|schema| schema.column_defs.is_none())
}
