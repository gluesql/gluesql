use {
    super::{expr::translate_expr, translate_idents, translate_object_name, TranslateError},
    crate::{
        ast::{
            Join, JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr, TableAlias,
            TableFactor, TableWithJoins, Values,
        },
        result::Result,
    },
    sqlparser::ast::{
        Expr as SqlExpr, Join as SqlJoin, JoinConstraint as SqlJoinConstraint,
        JoinOperator as SqlJoinOperator, Query as SqlQuery, Select as SqlSelect,
        SelectItem as SqlSelectItem, SetExpr as SqlSetExpr, TableAlias as SqlTableAlias,
        TableFactor as SqlTableFactor, TableWithJoins as SqlTableWithJoins,
    },
};

pub fn translate_query(sql_query: &SqlQuery) -> Result<Query> {
    let SqlQuery {
        body,
        limit,
        offset,
        ..
    } = sql_query;

    let body = translate_set_expr(body)?;
    let limit = limit.as_ref().map(translate_expr).transpose()?;
    let offset = offset
        .as_ref()
        .map(|offset| translate_expr(&offset.value))
        .transpose()?;

    Ok(Query {
        body,
        limit,
        offset,
    })
}

fn translate_set_expr(sql_set_expr: &SqlSetExpr) -> Result<SetExpr> {
    match sql_set_expr {
        SqlSetExpr::Select(select) => translate_select(select).map(Box::new).map(SetExpr::Select),
        SqlSetExpr::Values(values) => values
            .0
            .iter()
            .map(|items| items.iter().map(translate_expr).collect::<Result<_>>())
            .collect::<Result<_>>()
            .map(Values)
            .map(SetExpr::Values),
        _ => Err(TranslateError::UnsupportedQuerySetExpr(sql_set_expr.to_string()).into()),
    }
}

fn translate_select(sql_select: &SqlSelect) -> Result<Select> {
    let SqlSelect {
        projection,
        from,
        selection,
        group_by,
        having,
        ..
    } = sql_select;

    Ok(Select {
        projection: projection
            .iter()
            .map(translate_select_item)
            .collect::<Result<_>>()?,
        from: from
            .iter()
            .map(translate_table_with_joins)
            .collect::<Result<_>>()?,
        selection: selection.as_ref().map(translate_expr).transpose()?,
        group_by: group_by.iter().map(translate_expr).collect::<Result<_>>()?,
        having: having.as_ref().map(translate_expr).transpose()?,
    })
}

fn translate_select_item(sql_select_item: &SqlSelectItem) -> Result<SelectItem> {
    match sql_select_item {
        SqlSelectItem::UnnamedExpr(expr) => {
            let label = match expr {
                SqlExpr::CompoundIdentifier(idents) => idents
                    .last()
                    .map(|ident| ident.value.to_owned())
                    .unwrap_or_else(|| expr.to_string()),
                _ => expr.to_string(),
            };

            Ok(SelectItem::Expr {
                expr: translate_expr(expr)?,
                label,
            })
        }
        SqlSelectItem::ExprWithAlias { expr, alias } => {
            translate_expr(expr).map(|expr| SelectItem::Expr {
                expr,
                label: alias.value.to_owned(),
            })
        }
        SqlSelectItem::QualifiedWildcard(object_name) => Ok(SelectItem::QualifiedWildcard(
            translate_object_name(object_name),
        )),
        SqlSelectItem::Wildcard => Ok(SelectItem::Wildcard),
    }
}

fn translate_table_with_joins(sql_table_with_joins: &SqlTableWithJoins) -> Result<TableWithJoins> {
    let SqlTableWithJoins { relation, joins } = sql_table_with_joins;

    Ok(TableWithJoins {
        relation: translate_table_factor(relation)?,
        joins: joins.iter().map(translate_join).collect::<Result<_>>()?,
    })
}

fn translate_table_factor(sql_table_factor: &SqlTableFactor) -> Result<TableFactor> {
    match sql_table_factor {
        SqlTableFactor::Table { name, alias, .. } => Ok(TableFactor::Table {
            name: translate_object_name(name),
            alias: alias
                .as_ref()
                .map(|SqlTableAlias { name, columns }| TableAlias {
                    name: name.value.to_owned(),
                    columns: translate_idents(columns),
                }),
        }),
        _ => Err(TranslateError::UnsupportedQueryTableFactor(sql_table_factor.to_string()).into()),
    }
}

fn translate_join(sql_join: &SqlJoin) -> Result<Join> {
    let SqlJoin {
        relation,
        join_operator: sql_join_operator,
    } = sql_join;

    let translate_constraint = |sql_join_constraint: &SqlJoinConstraint| match sql_join_constraint {
        SqlJoinConstraint::On(expr) => translate_expr(&expr).map(JoinConstraint::On),
        SqlJoinConstraint::None => Ok(JoinConstraint::None),
        SqlJoinConstraint::Using(_) => {
            Err(TranslateError::UnsupportedJoinConstraint("USING".to_owned()).into())
        }
        SqlJoinConstraint::Natural => {
            Err(TranslateError::UnsupportedJoinConstraint("NATURAL".to_owned()).into())
        }
    };

    let join_operator = match sql_join_operator {
        SqlJoinOperator::Inner(sql_join_constraint) => {
            translate_constraint(sql_join_constraint).map(JoinOperator::Inner)
        }
        SqlJoinOperator::LeftOuter(sql_join_constraint) => {
            translate_constraint(sql_join_constraint).map(JoinOperator::LeftOuter)
        }
        _ => {
            Err(TranslateError::UnsupportedJoinOperator(format!("{:?}", sql_join_operator)).into())
        }
    }?;

    Ok(Join {
        relation: translate_table_factor(relation)?,
        join_operator,
    })
}
