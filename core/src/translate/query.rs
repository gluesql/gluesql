use {
    super::{
        function::translate_function_arg_exprs, translate_expr, translate_idents,
        translate_object_name, translate_order_by_expr, TranslateError,
    },
    crate::{
        ast::{
            AstLiteral, Dictionary, Expr, Join, JoinConstraint, JoinExecutor, JoinOperator, Query,
            Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins, Values,
        },
        result::Result,
    },
    sqlparser::ast::{
        Expr as SqlExpr, FunctionArg as SqlFunctionArg, GroupByExpr as SqlGroupByExpr,
        Join as SqlJoin, JoinConstraint as SqlJoinConstraint, JoinOperator as SqlJoinOperator,
        Query as SqlQuery, Select as SqlSelect, SelectItem as SqlSelectItem, SetExpr as SqlSetExpr,
        TableAlias as SqlTableAlias, TableFactor as SqlTableFactor,
        TableFunctionArgs as SqlTableFunctionArgs, TableWithJoins as SqlTableWithJoins,
    },
};

pub fn translate_query(sql_query: &SqlQuery) -> Result<Query> {
    let SqlQuery {
        body,
        order_by,
        limit,
        offset,
        ..
    } = sql_query;

    let body = translate_set_expr(body)?;
    let order_by = order_by
        .iter()
        .flat_map(|order_by| order_by.exprs.iter().map(translate_order_by_expr))
        .collect::<Result<_>>()?;
    let limit = limit.as_ref().map(translate_expr).transpose()?;
    let offset = offset
        .as_ref()
        .map(|offset| translate_expr(&offset.value))
        .transpose()?;

    Ok(Query {
        body,
        order_by,
        limit,
        offset,
    })
}

fn translate_set_expr(sql_set_expr: &SqlSetExpr) -> Result<SetExpr> {
    match sql_set_expr {
        SqlSetExpr::Select(select) => translate_select(select).map(Box::new).map(SetExpr::Select),
        SqlSetExpr::Values(sqlparser::ast::Values { rows, .. }) => rows
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
        distinct,
        ..
    } = sql_select;

    if from.len() > 1 {
        return Err(TranslateError::TooManyTables.into());
    }

    if distinct.is_some() {
        return Err(TranslateError::SelectDistinctNotSupported.into());
    }

    let from = match from.first() {
        Some(sql_table_with_joins) => translate_table_with_joins(sql_table_with_joins)?,
        None => TableWithJoins {
            relation: TableFactor::Series {
                alias: TableAlias {
                    name: "Series".to_owned(),
                    columns: Vec::new(),
                },
                size: Expr::Literal(AstLiteral::Number(1.into())),
            },
            joins: vec![],
        },
    };

    let group_by = match group_by {
        SqlGroupByExpr::Expressions(group_by, _group_by_with_modifiers) => group_by,
        SqlGroupByExpr::All(_group_by_with_modifiers) => {
            return Err(TranslateError::UnsupportedGroupByAll.into())
        }
    };

    Ok(Select {
        projection: projection
            .iter()
            .map(translate_select_item)
            .collect::<Result<_>>()?,
        from,
        selection: selection.as_ref().map(translate_expr).transpose()?,
        group_by: group_by.iter().map(translate_expr).collect::<Result<_>>()?,
        having: having.as_ref().map(translate_expr).transpose()?,
    })
}

pub fn translate_select_item(sql_select_item: &SqlSelectItem) -> Result<SelectItem> {
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
        SqlSelectItem::QualifiedWildcard(object_name, _) => Ok(SelectItem::QualifiedWildcard(
            translate_object_name(object_name)?,
        )),
        SqlSelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
    }
}

fn translate_table_with_joins(sql_table_with_joins: &SqlTableWithJoins) -> Result<TableWithJoins> {
    let SqlTableWithJoins { relation, joins } = sql_table_with_joins;

    Ok(TableWithJoins {
        relation: translate_table_factor(relation)?,
        joins: joins.iter().map(translate_join).collect::<Result<_>>()?,
    })
}

fn translate_table_alias(alias: &Option<SqlTableAlias>) -> Option<TableAlias> {
    alias
        .as_ref()
        .map(|SqlTableAlias { name, columns }| TableAlias {
            name: name.value.to_owned(),
            columns: translate_idents(columns),
        })
}

fn translate_table_factor(sql_table_factor: &SqlTableFactor) -> Result<TableFactor> {
    let translate_table_args = |args: &Vec<SqlFunctionArg>| -> Result<Expr> {
        let function_arg_exprs = args
            .iter()
            .map(|arg| match arg {
                SqlFunctionArg::Named { .. } => {
                    Err(TranslateError::NamedFunctionArgNotSupported.into())
                }
                SqlFunctionArg::Unnamed(arg_expr) => Ok(arg_expr),
            })
            .collect::<Result<Vec<_>>>()?;

        match translate_function_arg_exprs(function_arg_exprs)?.first() {
            Some(expr) => Ok(translate_expr(expr)?),
            None => Err(TranslateError::LackOfArgs.into()),
        }
    };

    match sql_table_factor {
        SqlTableFactor::Table {
            name, alias, args, ..
        } => {
            let object_name = translate_object_name(name)?.to_uppercase();
            let alias = translate_table_alias(alias);

            match (object_name.as_str(), args) {
                ("SERIES", Some(SqlTableFunctionArgs { args, .. })) => Ok(TableFactor::Series {
                    alias: alias_or_name(alias, object_name),
                    size: translate_table_args(args)?,
                }),
                ("GLUE_OBJECTS", _) => Ok(TableFactor::Dictionary {
                    dict: Dictionary::GlueObjects,
                    alias: alias_or_name(alias, object_name),
                }),
                ("GLUE_TABLES", _) => Ok(TableFactor::Dictionary {
                    dict: Dictionary::GlueTables,
                    alias: alias_or_name(alias, object_name),
                }),
                ("GLUE_INDEXES", _) => Ok(TableFactor::Dictionary {
                    dict: Dictionary::GlueIndexes,
                    alias: alias_or_name(alias, object_name),
                }),
                ("GLUE_TABLE_COLUMNS", _) => Ok(TableFactor::Dictionary {
                    dict: Dictionary::GlueTableColumns,
                    alias: alias_or_name(alias, object_name),
                }),
                _ => {
                    Ok(TableFactor::Table {
                        name: translate_object_name(name)?,
                        alias,
                        index: None, // query execution plan
                    })
                }
            }
        }
        SqlTableFactor::Derived {
            subquery, alias, ..
        } => {
            if let Some(alias) = alias {
                Ok(TableFactor::Derived {
                    subquery: translate_query(subquery)?,
                    alias: TableAlias {
                        name: alias.name.value.to_owned(),
                        columns: translate_idents(&alias.columns),
                    },
                })
            } else {
                Err(TranslateError::LackOfAlias.into())
            }
        }
        _ => Err(TranslateError::UnsupportedQueryTableFactor(sql_table_factor.to_string()).into()),
    }
}

pub fn alias_or_name(alias: Option<TableAlias>, name: String) -> TableAlias {
    alias.unwrap_or_else(|| TableAlias {
        name,
        columns: Vec::new(),
    })
}

fn translate_join(sql_join: &SqlJoin) -> Result<Join> {
    let SqlJoin {
        relation,
        join_operator: sql_join_operator,
        ..
    } = sql_join;

    let translate_constraint = |sql_join_constraint: &SqlJoinConstraint| match sql_join_constraint {
        SqlJoinConstraint::On(expr) => translate_expr(expr).map(JoinConstraint::On),
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
        join_executor: JoinExecutor::NestedLoop,
    })
}
