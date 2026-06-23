use {
    crate::{
        ast::Literal,
        data::{SCHEMALESS_DOC_COLUMN, Schema},
        plan::{
            ExprPlan, JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, ProjectionPlan,
            QueryPlan, SelectItemPlan, SelectPlan, SetExprPlan, TableFactorPlan,
            TableWithJoinsPlan, expr::visit_mut_expr,
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
    let state = match &mut query.body {
        SetExprPlan::Select(select) => {
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
        SetExprPlan::Values(_) => QueryRewriteState {
            rewrite_unqualified_identifiers: false,
            schemaless_aliases: HashSet::new(),
        },
    };

    for order_by in &mut query.order_by {
        transform_query_expr(schema_map, &mut order_by.expr, &state);
    }

    if let Some(limit) = query.limit.as_mut() {
        transform_query_expr(schema_map, limit, &state);
    }

    if let Some(offset) = query.offset.as_mut() {
        transform_query_expr(schema_map, offset, &state);
    }
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
    let root_wildcard_maps_to_doc =
        state.rewrite_unqualified_identifiers && select.from.joins.is_empty();
    let use_schemaless_map_projection = match &select.projection {
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
        select.projection = ProjectionPlan::SchemalessMap;
    }

    // Pass 1: rewrite identifier-based expressions.
    if let ProjectionPlan::SelectItems(projection) = &mut select.projection {
        for projection in projection {
            if let SelectItemPlan::Expr { expr, .. } = projection {
                transform_query_expr(schema_map, expr, state);
            }
        }
    }

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

    for group_by in &mut select.group_by {
        transform_query_expr(schema_map, group_by, state);
    }

    if let Some(having) = select.having.as_mut() {
        transform_query_expr(schema_map, having, state);
    }

    // Pass 2: rewrite wildcard projections.
    if let ProjectionPlan::SelectItems(projection) = &mut select.projection {
        for projection in projection {
            transform_wildcard_projection(
                projection,
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
