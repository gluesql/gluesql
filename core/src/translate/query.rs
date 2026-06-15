use {
    super::{
        ParamLiteral, TranslateError, function::translate_function_arg_exprs, translate_expr,
        translate_idents, translate_object_name, translate_order_by_expr,
    },
    crate::{
        ast::{
            Dictionary, Expr, Join, JoinConstraint, JoinOperator, Literal, Projection, Query,
            Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins, Values,
        },
        result::Result,
    },
    sqlparser::ast::{
        Distinct as SqlDistinct, Expr as SqlExpr, FunctionArg as SqlFunctionArg,
        GroupByExpr as SqlGroupByExpr, Join as SqlJoin, JoinConstraint as SqlJoinConstraint,
        JoinOperator as SqlJoinOperator, Query as SqlQuery, Select as SqlSelect,
        SelectItem as SqlSelectItem, SetExpr as SqlSetExpr, SetOperator as SqlSetOperator,
        SetQuantifier as SqlSetQuantifier, TableAlias as SqlTableAlias,
        TableFactor as SqlTableFactor, TableFunctionArgs as SqlTableFunctionArgs,
        TableWithJoins as SqlTableWithJoins,
    },
};

/// Translates a [`SqlQuery`] into `GlueSQL`'s [`Query`] using the supplied parameters.
///
/// # Errors
///
/// Returns an error when the SQL query uses clauses `GlueSQL` does not support or when translating
/// any expression within the query fails.
pub fn translate_query(sql_query: &SqlQuery, params: &[ParamLiteral]) -> Result<Query> {
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

    let body = translate_set_expr(body, params)?;
    let mut order_by_exprs = Vec::new();
    if let Some(order_by) = order_by {
        for expr in &order_by.exprs {
            order_by_exprs.push(translate_order_by_expr(expr, params)?);
        }
    }
    let limit = limit
        .as_ref()
        .map(|limit| translate_expr(limit, params))
        .transpose()?;
    let offset = offset
        .as_ref()
        .map(|offset| translate_expr(&offset.value, params))
        .transpose()?;

    Ok(Query {
        body,
        order_by: order_by_exprs,
        limit,
        offset,
    })
}

fn translate_set_expr(sql_set_expr: &SqlSetExpr, params: &[ParamLiteral]) -> Result<SetExpr> {
    match sql_set_expr {
        SqlSetExpr::Select(select) => translate_select(select, params)
            .map(Box::new)
            .map(SetExpr::Select),
        SqlSetExpr::Values(sqlparser::ast::Values { rows, .. }) => rows
            .iter()
            .map(|items| {
                items
                    .iter()
                    .map(|expr| translate_expr(expr, params))
                    .collect::<Result<_>>()
            })
            .collect::<Result<_>>()
            .map(Values)
            .map(SetExpr::Values),
        SqlSetExpr::SetOperation {
            op: SqlSetOperator::Union,
            set_quantifier,
            left,
            right,
        } => {
            let all = match set_quantifier {
                SqlSetQuantifier::All => true,
                SqlSetQuantifier::None | SqlSetQuantifier::Distinct => false,
                SqlSetQuantifier::ByName
                | SqlSetQuantifier::AllByName
                | SqlSetQuantifier::DistinctByName => {
                    return Err(
                        TranslateError::UnsupportedQuerySetExpr(sql_set_expr.to_string()).into(),
                    );
                }
            };
            let left = Box::new(translate_set_expr(left, params)?);
            let right = Box::new(translate_set_expr(right, params)?);
            Ok(SetExpr::Union { left, right, all })
        }
        _ => Err(TranslateError::UnsupportedQuerySetExpr(sql_set_expr.to_string()).into()),
    }
}

fn translate_select(sql_select: &SqlSelect, params: &[ParamLiteral]) -> Result<Select> {
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
                size: Expr::Literal(Literal::Number(1.into())),
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
        projection: Projection::SelectItems(
            projection
                .iter()
                .map(|item| translate_select_item(item, params))
                .collect::<Result<_>>()?,
        ),
        from,
        selection: selection
            .as_ref()
            .map(|expr| translate_expr(expr, params))
            .transpose()?,
        group_by: group_by
            .iter()
            .map(|expr| translate_expr(expr, params))
            .collect::<Result<_>>()?,
        having: having
            .as_ref()
            .map(|expr| translate_expr(expr, params))
            .transpose()?,
    })
}

/// Translates a [`SqlSelectItem`] into `GlueSQL`'s [`SelectItem`].
///
/// # Errors
///
/// Returns an error when converting the underlying expression fails or when a
/// qualified wildcard references an unsupported object name.
pub fn translate_select_item(
    sql_select_item: &SqlSelectItem,
    params: &[ParamLiteral],
) -> Result<SelectItem> {
    match sql_select_item {
        SqlSelectItem::UnnamedExpr(expr) => {
            let label = match expr {
                SqlExpr::CompoundIdentifier(idents) => idents
                    .last()
                    .map_or_else(|| expr.to_string(), |ident| ident.value.clone()),
                _ => expr.to_string(),
            };

            Ok(SelectItem::Expr {
                expr: translate_expr(expr, params)?,
                label,
            })
        }
        SqlSelectItem::ExprWithAlias { expr, alias } => {
            translate_expr(expr, params).map(|expr| SelectItem::Expr {
                expr,
                label: alias.value.clone(),
            })
        }
        SqlSelectItem::QualifiedWildcard(object_name, _) => Ok(SelectItem::QualifiedWildcard(
            translate_object_name(object_name)?,
        )),
        SqlSelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
    }
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

fn translate_table_alias(alias: Option<&SqlTableAlias>) -> Option<TableAlias> {
    alias.map(|SqlTableAlias { name, columns }| TableAlias {
        name: name.value.clone(),
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
            Some(expr) => Ok(translate_expr(expr, params)?),
            None => Err(TranslateError::LackOfArgs.into()),
        }
    };

    match sql_table_factor {
        SqlTableFactor::Table {
            name, alias, args, ..
        } => {
            let object_name = translate_object_name(name)?.to_uppercase();
            let alias = translate_table_alias(alias.as_ref());

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
                _ => Ok(TableFactor::Table {
                    name: translate_object_name(name)?,
                    alias,
                }),
            }
        }
        SqlTableFactor::Derived {
            subquery, alias, ..
        } => {
            if let Some(alias) = alias {
                Ok(TableFactor::Derived {
                    subquery: translate_query(subquery, params)?,
                    alias: TableAlias {
                        name: alias.name.value.clone(),
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
        SqlJoinConstraint::On(expr) => translate_expr(expr, params).map(JoinConstraint::On),
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
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            ast::{Expr, Literal, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins},
            data::Value,
            parse_sql::{parse, parse_query},
            result::Error,
            translate::{IntoParamLiteral, NO_PARAMS},
        },
        sqlparser::ast::Statement as SqlStatement,
    };

    fn assert_query_error(sql: &str, expected: TranslateError) {
        let mut parsed = parse(sql).expect("parse");
        let statement = parsed.remove(0);
        let SqlStatement::Query(query) = statement else {
            panic!("expected query statement: {sql}");
        };

        let actual = translate_query(&query, NO_PARAMS);
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
        let query = parse_query("SELECT $1, $2").expect("parse placeholder query");
        let params = [1_i64.into_param_literal(), "GlueSQL".into_param_literal()];
        let translated = translate_query(query.as_ref(), &params).expect("translate");

        let expected = Query {
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                projection: Projection::SelectItems(vec![
                    SelectItem::Expr {
                        expr: Expr::Value(Value::I64(1)),
                        label: "$1".to_owned(),
                    },
                    SelectItem::Expr {
                        expr: Expr::Value(Value::Str("GlueSQL".to_owned())),
                        label: "$2".to_owned(),
                    },
                ]),
                from: TableWithJoins {
                    relation: TableFactor::Series {
                        alias: TableAlias {
                            name: "Series".to_owned(),
                            columns: Vec::new(),
                        },
                        size: Expr::Literal(Literal::Number(1.into())),
                    },
                    joins: Vec::new(),
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            })),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };

        assert_eq!(translated, expected);
    }

    fn simple_select_query(table: &str, column: &str) -> Query {
        use crate::ast::Projection;

        Query {
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                projection: Projection::SelectItems(vec![SelectItem::Expr {
                    expr: Expr::Identifier(column.to_owned()),
                    label: column.to_owned(),
                }]),
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: table.to_owned(),
                        alias: None,
                    },
                    joins: Vec::new(),
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            })),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    #[test]
    fn translate_union_produces_union_set_expr() {
        let query = parse_query("SELECT id FROM A UNION SELECT id FROM B").expect("parse");
        let translated = translate_query(query.as_ref(), NO_PARAMS).expect("translate");

        let left = simple_select_query("A", "id");
        let right = simple_select_query("B", "id");

        let expected = Query {
            body: SetExpr::Union {
                left: Box::new(left.body),
                right: Box::new(right.body),
                all: false,
            },
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };

        assert_eq!(translated, expected);
    }

    #[test]
    fn translate_union_all_sets_all_flag() {
        let query = parse_query("SELECT id FROM A UNION ALL SELECT id FROM B").expect("parse");
        let translated = translate_query(query.as_ref(), NO_PARAMS).expect("translate");

        assert!(matches!(&translated.body, SetExpr::Union { all: true, .. }));
    }

    #[test]
    fn translate_union_default_is_distinct() {
        let query = parse_query("SELECT id FROM A UNION SELECT id FROM B").expect("parse");
        let translated = translate_query(query.as_ref(), NO_PARAMS).expect("translate");

        assert!(matches!(
            &translated.body,
            SetExpr::Union { all: false, .. }
        ));
    }

    #[test]
    fn union_set_expr_to_sql_roundtrip() {
        use crate::ast::ToSql;

        let sql = "SELECT id FROM A UNION SELECT id FROM B";
        let query = parse_query(sql).expect("parse");
        let translated = translate_query(query.as_ref(), NO_PARAMS).expect("translate");

        let result = translated.to_sql();
        // The round-trip should contain UNION without ALL.
        assert!(result.contains("UNION"), "expected UNION in: {result}");
        assert!(
            !result.contains("UNION ALL"),
            "should not contain ALL: {result}"
        );
    }

    #[test]
    fn union_all_set_expr_to_sql_roundtrip() {
        use crate::ast::ToSql;

        let sql = "SELECT id FROM A UNION ALL SELECT id FROM B";
        let query = parse_query(sql).expect("parse");
        let translated = translate_query(query.as_ref(), NO_PARAMS).expect("translate");

        let result = translated.to_sql();
        assert!(
            result.contains("UNION ALL"),
            "expected UNION ALL in: {result}"
        );
    }

    #[test]
    fn unsupported_set_op_intersect_returns_error() {
        let result = parse_query("SELECT 1 INTERSECT SELECT 1")
            .and_then(|q| translate_query(q.as_ref(), NO_PARAMS));

        assert!(
            matches!(
                result,
                Err(Error::Translate(TranslateError::UnsupportedQuerySetExpr(_)))
            ),
            "expected UnsupportedQuerySetExpr error, got: {result:?}"
        );
    }

    #[test]
    fn unsupported_set_op_except_returns_error() {
        let result = parse_query("SELECT 1 EXCEPT SELECT 1")
            .and_then(|q| translate_query(q.as_ref(), NO_PARAMS));

        assert!(
            matches!(
                result,
                Err(Error::Translate(TranslateError::UnsupportedQuerySetExpr(_)))
            ),
            "expected UnsupportedQuerySetExpr error, got: {result:?}"
        );
    }

    #[test]
    fn union_by_name_variants_are_unsupported() {
        // UNION BY NAME / UNION ALL BY NAME / UNION DISTINCT BY NAME are
        // DuckDB/Snowflake extensions that GlueSQL does not support.
        // Each must return UnsupportedQuerySetExpr rather than silently
        // treating the BY NAME qualifier as a plain UNION DISTINCT.
        //
        // Note: sqlparser only parses these with the DuckDB dialect, so
        // we construct the AST directly here instead of going through parse_query.
        use {
            crate::translate::translate_query,
            sqlparser::ast::{
                Query as SqlQuery, SetExpr as SqlSetExpr, SetOperator as SqlSetOperator,
                SetQuantifier as SqlSetQuantifier,
            },
        };

        let make_query = |quantifier: SqlSetQuantifier| -> SqlQuery {
            let one = Box::new(SqlSetExpr::Query(Box::new(
                sqlparser::parser::Parser::parse_sql(
                    &sqlparser::dialect::GenericDialect {},
                    "SELECT 1",
                )
                .unwrap()
                .into_iter()
                .next()
                .and_then(|s| match s {
                    sqlparser::ast::Statement::Query(q) => Some(*q),
                    _ => None,
                })
                .unwrap(),
            )));
            SqlQuery {
                with: None,
                body: Box::new(SqlSetExpr::SetOperation {
                    op: SqlSetOperator::Union,
                    set_quantifier: quantifier,
                    left: one.clone(),
                    right: one,
                }),
                order_by: None,
                limit: None,
                limit_by: vec![],
                offset: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            }
        };

        for quantifier in [
            SqlSetQuantifier::ByName,
            SqlSetQuantifier::AllByName,
            SqlSetQuantifier::DistinctByName,
        ] {
            let q = make_query(quantifier);
            let result = translate_query(&q, NO_PARAMS);
            assert!(
                matches!(
                    result,
                    Err(Error::Translate(TranslateError::UnsupportedQuerySetExpr(_)))
                ),
                "expected UnsupportedQuerySetExpr for BY NAME variant, got: {result:?}"
            );
        }
    }
}
