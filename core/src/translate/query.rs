use {
    super::{
        ParamLiteral, TranslateError, function::translate_function_arg_exprs,
        translate_expr_with_params, translate_idents, translate_object_name,
        translate_order_by_expr_with_params,
    },
    crate::{
        ast::{
            AstLiteral, Dictionary, Expr, Join, JoinConstraint, JoinExecutor, JoinOperator, Query,
            Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins, Values,
        },
        result::Result,
    },
    sqlparser::ast::{
        Distinct as SqlDistinct, Expr as SqlExpr, FunctionArg as SqlFunctionArg,
        GroupByExpr as SqlGroupByExpr, Join as SqlJoin, JoinConstraint as SqlJoinConstraint,
        JoinOperator as SqlJoinOperator, Query as SqlQuery, Select as SqlSelect,
        SelectItem as SqlSelectItem, SetExpr as SqlSetExpr, TableAlias as SqlTableAlias,
        TableFactor as SqlTableFactor, TableFunctionArgs as SqlTableFunctionArgs,
        TableWithJoins as SqlTableWithJoins,
    },
};

pub(crate) fn translate_query_with_params(
    sql_query: &SqlQuery,
    params: &[ParamLiteral],
) -> Result<Query> {
    let SqlQuery {
        with,
        body,
        order_by,
        limit,
        offset,
        fetch,
        locks,
        ..
    } = sql_query;

    let violation = if with.is_some() {
        Some("WITH clause")
    } else if fetch.is_some() {
        Some("FETCH clause")
    } else if !locks.is_empty() {
        Some("LOCK clause")
    } else {
        None
    };

    if let Some(reason) = violation {
        return Err(TranslateError::UnsupportedQueryOption(reason).into());
    }

    let body = translate_set_expr_with_params(params, body)?;
    let mut order_by_exprs = Vec::new();
    for clause in order_by.iter() {
        for expr in clause.exprs.iter() {
            order_by_exprs.push(translate_order_by_expr_with_params(expr, params)?);
        }
    }
    let limit = limit
        .as_ref()
        .map(|limit| translate_expr_with_params(limit, params))
        .transpose()?;
    let offset = offset
        .as_ref()
        .map(|offset| translate_expr_with_params(&offset.value, params))
        .transpose()?;

    Ok(Query {
        body,
        order_by: order_by_exprs,
        limit,
        offset,
    })
}

pub fn translate_query(sql_query: &SqlQuery) -> Result<Query> {
    const NO_PARAMS: [ParamLiteral; 0] = [];
    translate_query_with_params(sql_query, &NO_PARAMS)
}

fn translate_set_expr_with_params(
    params: &[ParamLiteral],
    sql_set_expr: &SqlSetExpr,
) -> Result<SetExpr> {
    match sql_set_expr {
        SqlSetExpr::Select(select) => translate_select_with_params(params, select)
            .map(Box::new)
            .map(SetExpr::Select),
        SqlSetExpr::Values(sqlparser::ast::Values { rows, .. }) => rows
            .iter()
            .map(|items| {
                items
                    .iter()
                    .map(|expr| translate_expr_with_params(expr, params))
                    .collect::<Result<_>>()
            })
            .collect::<Result<_>>()
            .map(Values)
            .map(SetExpr::Values),
        _ => Err(TranslateError::UnsupportedQuerySetExpr(sql_set_expr.to_string()).into()),
    }
}

fn translate_select_with_params(params: &[ParamLiteral], sql_select: &SqlSelect) -> Result<Select> {
    let SqlSelect {
        projection,
        from,
        selection,
        group_by,
        having,
        distinct,
        into,
        named_window,
        ..
    } = sql_select;

    if into.is_some() {
        return Err(TranslateError::UnsupportedSelectOption("INTO clause").into());
    } else if !named_window.is_empty() {
        return Err(TranslateError::UnsupportedSelectOption("WINDOW clause").into());
    }

    if from.len() > 1 {
        return Err(TranslateError::TooManyTables.into());
    }

    let distinct = match distinct {
        Some(SqlDistinct::Distinct) => true,
        Some(SqlDistinct::On(_)) => {
            return Err(TranslateError::SelectDistinctOnNotSupported.into());
        }
        None => false,
    };

    let from = match from.first() {
        Some(sql_table_with_joins) => translate_table_with_joins(params, sql_table_with_joins)?,
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
            return Err(TranslateError::UnsupportedGroupByAll.into());
        }
    };

    Ok(Select {
        distinct,
        projection: projection
            .iter()
            .map(|item| translate_select_item_with_params(params, item))
            .collect::<Result<_>>()?,
        from,
        selection: selection
            .as_ref()
            .map(|expr| translate_expr_with_params(expr, params))
            .transpose()?,
        group_by: group_by
            .iter()
            .map(|expr| translate_expr_with_params(expr, params))
            .collect::<Result<_>>()?,
        having: having
            .as_ref()
            .map(|expr| translate_expr_with_params(expr, params))
            .transpose()?,
    })
}

pub(crate) fn translate_select_item_with_params(
    params: &[ParamLiteral],
    sql_select_item: &SqlSelectItem,
) -> Result<SelectItem> {
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
                expr: translate_expr_with_params(expr, params)?,
                label,
            })
        }
        SqlSelectItem::ExprWithAlias { expr, alias } => translate_expr_with_params(expr, params)
            .map(|expr| SelectItem::Expr {
                expr,
                label: alias.value.to_owned(),
            }),
        SqlSelectItem::QualifiedWildcard(object_name, _) => Ok(SelectItem::QualifiedWildcard(
            translate_object_name(object_name)?,
        )),
        SqlSelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
    }
}

pub fn translate_select_item(sql_select_item: &SqlSelectItem) -> Result<SelectItem> {
    const NO_PARAMS: [ParamLiteral; 0] = [];
    translate_select_item_with_params(&NO_PARAMS, sql_select_item)
}

fn translate_table_with_joins(
    params: &[ParamLiteral],
    sql_table_with_joins: &SqlTableWithJoins,
) -> Result<TableWithJoins> {
    let SqlTableWithJoins { relation, joins } = sql_table_with_joins;

    Ok(TableWithJoins {
        relation: translate_table_factor(params, relation)?,
        joins: joins
            .iter()
            .map(|join| translate_join(params, join))
            .collect::<Result<_>>()?,
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

fn translate_table_factor(
    params: &[ParamLiteral],
    sql_table_factor: &SqlTableFactor,
) -> Result<TableFactor> {
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
            Some(expr) => Ok(translate_expr_with_params(expr, params)?),
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
                    subquery: translate_query_with_params(subquery, params)?,
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

fn translate_join(params: &[ParamLiteral], sql_join: &SqlJoin) -> Result<Join> {
    let SqlJoin {
        relation,
        join_operator: sql_join_operator,
        ..
    } = sql_join;

    let translate_constraint = |sql_join_constraint: &SqlJoinConstraint| match sql_join_constraint {
        SqlJoinConstraint::On(expr) => {
            translate_expr_with_params(expr, params).map(JoinConstraint::On)
        }
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
        _ => Err(TranslateError::UnsupportedJoinOperator(format!("{sql_join_operator:?}")).into()),
    }?;

    Ok(Join {
        relation: translate_table_factor(params, relation)?,
        join_operator,
        join_executor: JoinExecutor::NestedLoop,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{parse_sql::parse, result::Error},
        sqlparser::ast::Statement as SqlStatement,
    };

    fn assert_query_error(sql: &str, expected: TranslateError) {
        let mut parsed = parse(sql).expect("parse");
        let statement = parsed.remove(0);
        let query = match statement {
            SqlStatement::Query(query) => query,
            _ => panic!("expected query statement: {sql}"),
        };

        let actual = translate_query(&query);
        let expected = Err::<Query, Error>(Error::Translate(expected));
        assert_eq!(actual, expected, "translate_query mismatch for `{sql}`");
    }

    #[test]
    fn query_options_rejected() {
        assert_query_error(
            "WITH t AS (SELECT 1) SELECT * FROM t",
            TranslateError::UnsupportedQueryOption("WITH clause"),
        );
        assert_query_error(
            "SELECT * FROM Foo FETCH FIRST 1 ROW ONLY",
            TranslateError::UnsupportedQueryOption("FETCH clause"),
        );
        assert_query_error(
            "SELECT * FROM Foo FOR UPDATE",
            TranslateError::UnsupportedQueryOption("LOCK clause"),
        );
    }

    #[test]
    fn select_options_rejected() {
        assert_query_error(
            "SELECT * INTO Foo FROM Bar",
            TranslateError::UnsupportedSelectOption("INTO clause"),
        );
        assert_query_error(
            "SELECT * FROM Foo WINDOW w AS (PARTITION BY id)",
            TranslateError::UnsupportedSelectOption("WINDOW clause"),
        );
    }

    #[test]
    #[should_panic(expected = "expected query statement")]
    fn query_option_helper_panics_on_non_query() {
        assert_query_error(
            "INSERT INTO Foo VALUES (1)",
            TranslateError::UnsupportedQueryOption("unused"),
        );
    }

    #[test]
    fn translate_binds_indexed_placeholders() {
        let mut parsed = parse("SELECT $1, $2").expect("parse placeholder query");
        let statement = parsed.remove(0);
        let query = match statement {
            SqlStatement::Query(query) => query,
            _ => unreachable!("expected query statement"),
        };

        let params = [ParamLiteral::from(1_i64), ParamLiteral::from("GlueSQL")];
        let translated = translate_query_with_params(&query, &params).expect("translate");

        match translated.body {
            SetExpr::Select(select) => {
                assert_eq!(select.projection.len(), 2);

                match &select.projection[0] {
                    SelectItem::Expr { expr, .. } => {
                        assert_eq!(expr, &Expr::Literal(AstLiteral::Number(1.into())));
                    }
                    item => panic!("unexpected select item: {item:?}"),
                }

                match &select.projection[1] {
                    SelectItem::Expr { expr, .. } => {
                        assert_eq!(
                            expr,
                            &Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned()))
                        );
                    }
                    item => panic!("unexpected select item: {item:?}"),
                }
            }
            body => panic!("expected select body, got {body:?}"),
        }
    }
}
