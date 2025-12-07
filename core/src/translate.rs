mod data_type;
mod ddl;
mod error;
mod expr;
mod function;
mod literal;
mod operator;
mod param;
mod query;

pub use self::{
    data_type::translate_data_type,
    ddl::translate_column_def,
    error::TranslateError,
    expr::{translate_expr, translate_order_by_expr},
    param::{IntoParamLiteral, ParamLiteral},
    query::{alias_or_name, translate_query, translate_select_item},
};

use {
    crate::{
        ast::{Assignment, Expr, ForeignKey, ReferentialAction, Statement, Variable},
        result::Result,
    },
    ddl::{translate_alter_table_operation, translate_operate_function_arg},
    sqlparser::ast::{
        Assignment as SqlAssignment, AssignmentTarget as SqlAssignmentTarget,
        CommentDef as SqlCommentDef, CreateFunctionBody as SqlCreateFunctionBody,
        CreateIndex as SqlCreateIndex, CreateTable as SqlCreateTable, Delete as SqlDelete,
        FromTable as SqlFromTable, Ident as SqlIdent, Insert as SqlInsert,
        ObjectName as SqlObjectName, ObjectType as SqlObjectType,
        ReferentialAction as SqlReferentialAction, Statement as SqlStatement,
        TableConstraint as SqlTableConstraint, TableFactor, TableWithJoins,
    },
    std::num::NonZeroUsize,
};

pub(crate) const NO_PARAMS: &[ParamLiteral] = &[];

/// Translates a [`SqlStatement`] into `GlueSQL`'s [`Statement`] without parameters.
///
/// This is a convenience wrapper around [`translate_with_params`] that invokes the
/// parameter-aware variant with an empty parameter list.
///
/// # Errors
///
/// Returns an error when the SQL statement includes syntax or features `GlueSQL` does not support.
pub fn translate(sql_statement: &SqlStatement) -> Result<Statement> {
    translate_with_params(sql_statement, NO_PARAMS)
}

/// Translates a [`SqlStatement`] into `GlueSQL`'s [`Statement`] using the supplied parameters.
///
/// # Errors
///
/// Returns an error when converting the provided parameters fails or when the SQL statement
/// uses syntax `GlueSQL` does not support.
pub fn translate_with_params(
    sql_statement: &SqlStatement,
    params: &[ParamLiteral],
) -> Result<Statement> {
    match sql_statement {
        SqlStatement::Query(query) => translate_query(query, params).map(Statement::Query),
        SqlStatement::Insert(SqlInsert {
            table_name,
            columns,
            source,
            returning,
            on,
            table_alias,
            partitioned,
            overwrite,
            table,
            ..
        }) => {
            let violation = if returning.is_some() {
                Some("RETURNING clause")
            } else if on.is_some() {
                Some("ON CONFLICT clause")
            } else if table_alias.is_some() {
                Some("table alias")
            } else if partitioned.is_some() {
                Some("PARTITION clause")
            } else if *overwrite {
                Some("OVERWRITE clause")
            } else if *table {
                Some("TABLE keyword")
            } else {
                None
            };

            if let Some(reason) = violation {
                return Err(TranslateError::UnsupportedInsertOption(reason).into());
            }

            let table_name = translate_object_name(table_name)?;
            let columns = translate_idents(columns);
            let source = match source.as_deref() {
                Some(query) => translate_query(query, params)?,
                None => {
                    return Err(TranslateError::DefaultValuesOnInsertNotSupported(
                        table_name.clone(),
                    )
                    .into());
                }
            };

            Ok(Statement::Insert {
                table_name,
                columns,
                source,
            })
        }
        SqlStatement::Update {
            table,
            assignments,
            selection,
            from,
            returning,
            ..
        } => {
            let violation = if from.is_some() {
                Some("FROM clause")
            } else if returning.is_some() {
                Some("RETURNING clause")
            } else {
                None
            };

            if let Some(reason) = violation {
                return Err(TranslateError::UnsupportedUpdateOption(reason).into());
            }

            Ok(Statement::Update {
                table_name: translate_table_with_join(table)?,
                assignments: assignments
                    .iter()
                    .map(|assignment| translate_assignment(assignment, params))
                    .collect::<Result<_>>()?,
                selection: selection
                    .as_ref()
                    .map(|expr| translate_expr(expr, params))
                    .transpose()?,
            })
        }
        SqlStatement::Delete(SqlDelete {
            from,
            using,
            selection,
            returning,
            order_by,
            limit,
            ..
        }) => {
            let violation = if using.is_some() {
                Some("USING clause")
            } else if returning.is_some() {
                Some("RETURNING clause")
            } else if !order_by.is_empty() {
                Some("ORDER BY clause")
            } else if limit.is_some() {
                Some("LIMIT clause")
            } else {
                None
            };

            if let Some(reason) = violation {
                return Err(TranslateError::UnsupportedDeleteOption(reason).into());
            }

            let from = match from {
                SqlFromTable::WithFromKeyword(from) => from,
                SqlFromTable::WithoutKeyword(_) => {
                    return Err(TranslateError::UnreachableOmittingFromInDelete.into());
                }
            };
            let table_name = from
                .iter()
                .map(translate_table_with_join)
                .next()
                .ok_or(TranslateError::UnreachableEmptyTable)??;

            Ok(Statement::Delete {
                table_name,
                selection: selection
                    .as_ref()
                    .map(|expr| translate_expr(expr, params))
                    .transpose()?,
            })
        }
        SqlStatement::CreateTable(SqlCreateTable {
            if_not_exists,
            name,
            columns,
            query,
            engine,
            constraints,
            comment,
            ..
        }) => {
            let columns = columns
                .iter()
                .map(|column_def| translate_column_def(column_def, params))
                .collect::<Result<Vec<_>>>()?;

            let columns = (!columns.is_empty()).then_some(columns);

            let name = translate_object_name(name)?;

            let foreign_keys = constraints
                .iter()
                .map(translate_foreign_key)
                .collect::<Result<Vec<_>>>()?;

            Ok(Statement::CreateTable {
                if_not_exists: *if_not_exists,
                name,
                columns,
                source: match query {
                    Some(v) => Some(translate_query(v, params).map(Box::new)?),
                    None => None,
                },
                engine: engine
                    .as_ref()
                    .map(|table_engine| table_engine.name.clone()),
                foreign_keys,
                comment: comment.as_ref().map(|comment| match comment {
                    SqlCommentDef::WithEq(comment)
                    | SqlCommentDef::WithoutEq(comment)
                    | SqlCommentDef::AfterColumnDefsWithoutEq(comment) => comment.to_owned(),
                }),
            })
        }
        SqlStatement::AlterTable {
            name, operations, ..
        } => {
            if operations.len() > 1 {
                return Err(TranslateError::UnsupportedMultipleAlterTableOperations.into());
            }

            let operation = operations
                .iter()
                .next()
                .ok_or(TranslateError::UnreachableEmptyAlterTableOperation)?;

            Ok(Statement::AlterTable {
                name: translate_object_name(name)?,
                operation: translate_alter_table_operation(operation, params)?,
            })
        }
        SqlStatement::Drop {
            object_type: SqlObjectType::Table,
            if_exists,
            names,
            cascade,
            ..
        } => Ok(Statement::DropTable {
            if_exists: *if_exists,
            names: names
                .iter()
                .map(translate_object_name)
                .collect::<Result<Vec<_>>>()?,
            cascade: *cascade,
        }),
        SqlStatement::DropFunction {
            if_exists,
            func_desc,
            ..
        } => Ok(Statement::DropFunction {
            if_exists: *if_exists,
            names: func_desc
                .iter()
                .map(|v| translate_object_name(&v.name))
                .collect::<Result<Vec<_>>>()?,
        }),
        SqlStatement::CreateIndex(SqlCreateIndex {
            name,
            table_name,
            columns,
            ..
        }) => {
            if columns.len() > 1 {
                return Err(TranslateError::CompositeIndexNotSupported.into());
            }

            let Some(name) = name else {
                return Err(TranslateError::UnsupportedUnnamedIndex.into());
            };

            let name = translate_object_name(name)?;

            if name.to_uppercase() == "PRIMARY" {
                return Err(TranslateError::ReservedIndexName(name).into());
            }

            Ok(Statement::CreateIndex {
                name,
                table_name: translate_object_name(table_name)?,
                column: translate_order_by_expr(&columns[0], params)?,
            })
        }
        SqlStatement::Drop {
            object_type: SqlObjectType::Index,
            names,
            ..
        } => {
            if names.len() > 1 {
                return Err(TranslateError::TooManyParamsInDropIndex.into());
            }

            let object_name = &names[0].0;
            if object_name.len() != 2 {
                return Err(TranslateError::InvalidParamsInDropIndex.into());
            }

            let table_name = object_name[0].value.clone();
            let name = object_name[1].value.clone();

            if name.to_uppercase() == "PRIMARY" {
                return Err(TranslateError::CannotDropPrimary.into());
            }

            Ok(Statement::DropIndex { name, table_name })
        }
        SqlStatement::StartTransaction { .. } => Ok(Statement::StartTransaction),
        SqlStatement::Commit { .. } => Ok(Statement::Commit),
        SqlStatement::Rollback { .. } => Ok(Statement::Rollback),
        SqlStatement::ShowTables {
            filter: None,
            db_name: None,
            ..
        } => Ok(Statement::ShowVariable(Variable::Tables)),
        SqlStatement::ShowFunctions { filter: None } => {
            Ok(Statement::ShowVariable(Variable::Functions))
        }
        SqlStatement::ShowVariable { variable } => match (variable.len(), variable.first()) {
            (1, Some(keyword)) => match keyword.value.to_uppercase().as_str() {
                "VERSION" => Ok(Statement::ShowVariable(Variable::Version)),
                v => Err(TranslateError::UnsupportedShowVariableKeyword(v.to_owned()).into()),
            },
            (3, Some(keyword)) => match keyword.value.to_uppercase().as_str() {
                "INDEXES" => match variable.get(2) {
                    Some(tablename) => Ok(Statement::ShowIndexes(tablename.value.clone())),
                    _ => Err(TranslateError::UnsupportedShowVariableStatement(
                        sql_statement.to_string(),
                    )
                    .into()),
                },
                _ => Err(TranslateError::UnsupportedShowVariableStatement(
                    sql_statement.to_string(),
                )
                .into()),
            },
            _ => Err(
                TranslateError::UnsupportedShowVariableStatement(sql_statement.to_string()).into(),
            ),
        },
        SqlStatement::ShowColumns { table_name, .. } => Ok(Statement::ShowColumns {
            table_name: translate_object_name(table_name)?,
        }),
        SqlStatement::CreateFunction {
            or_replace,
            name,
            args,
            function_body: Some(SqlCreateFunctionBody::Return(return_)),
            ..
        } => {
            let args = args
                .as_ref()
                .map(|args| {
                    args.iter()
                        .map(|arg| translate_operate_function_arg(arg, params))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            Ok(Statement::CreateFunction {
                or_replace: *or_replace,
                name: translate_object_name(name)?,
                args: args.unwrap_or_default(),
                return_: translate_expr(return_, params)?,
            })
        }
        SqlStatement::CreateFunction { .. } => {
            Err(TranslateError::UnsupportedEmptyFunctionBody.into())
        }
        _ => Err(TranslateError::UnsupportedStatement(sql_statement.to_string()).into()),
    }
}

pub(crate) fn bind_placeholder(params: &[ParamLiteral], placeholder: &str) -> Result<Expr> {
    let invalid_placeholder = || TranslateError::InvalidPlaceholder {
        placeholder: placeholder.to_owned(),
    };

    let index = placeholder
        .strip_prefix('$')
        .ok_or_else(invalid_placeholder)?
        .parse::<NonZeroUsize>()
        .map_err(|_| invalid_placeholder())?;

    let literal =
        params
            .get(index.get() - 1)
            .cloned()
            .ok_or(TranslateError::ParameterIndexOutOfRange {
                index: index.get(),
                len: params.len(),
            })?;

    Ok(literal.into_expr())
}

/// Translates a [`SqlAssignment`] into `GlueSQL`'s [`Assignment`].
///
/// # Errors
///
/// Returns an error when tuple or compound identifiers appear in the assignment
/// target, when the identifier list is empty, or when translating the RHS
/// expression fails.
pub fn translate_assignment(
    sql_assignment: &SqlAssignment,
    params: &[ParamLiteral],
) -> Result<Assignment> {
    let SqlAssignment { target, value } = sql_assignment;

    let id = match target {
        SqlAssignmentTarget::Tuple(_) => {
            return Err(TranslateError::TupleAssignmentOnUpdateNotSupported(
                sql_assignment.to_string(),
            )
            .into());
        }
        SqlAssignmentTarget::ColumnName(SqlObjectName(id)) => id,
    };

    if id.len() > 1 {
        return Err(
            TranslateError::CompoundIdentOnUpdateNotSupported(sql_assignment.to_string()).into(),
        );
    }

    Ok(Assignment {
        id: id
            .first()
            .ok_or(TranslateError::UnreachableEmptyIdent)?
            .value
            .clone(),
        value: translate_expr(value, params)?,
    })
}

fn translate_table_with_join(table: &TableWithJoins) -> Result<String> {
    if !table.joins.is_empty() {
        return Err(TranslateError::JoinOnUpdateNotSupported.into());
    }
    match &table.relation {
        TableFactor::Table { name, .. } => translate_object_name(name),
        t => Err(TranslateError::UnsupportedTableFactor(t.to_string()).into()),
    }
}

fn translate_object_name(sql_object_name: &SqlObjectName) -> Result<String> {
    let sql_object_name = &sql_object_name.0;
    if sql_object_name.len() > 1 {
        let compound_object_name = translate_idents(sql_object_name).join(".");
        return Err(TranslateError::CompoundObjectNotSupported(compound_object_name).into());
    }

    sql_object_name
        .first()
        .map(|v| v.value.clone())
        .ok_or_else(|| TranslateError::UnreachableEmptyObject.into())
}

pub fn translate_idents(idents: &[SqlIdent]) -> Vec<String> {
    idents.iter().map(|v| v.value.clone()).collect()
}

pub fn translate_referential_action(
    action: &Option<SqlReferentialAction>,
) -> Result<ReferentialAction> {
    use SqlReferentialAction::*;

    let action = action.unwrap_or(NoAction);

    match action {
        NoAction | Restrict => Ok(ReferentialAction::NoAction),
        _ => Err(TranslateError::UnsupportedConstraint(action.to_string()).into()),
    }
}

pub fn translate_foreign_key(table_constraint: &SqlTableConstraint) -> Result<ForeignKey> {
    match table_constraint {
        SqlTableConstraint::ForeignKey {
            name,
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            on_update,
            ..
        } => {
            let referencing_column_name = columns.first().map(|i| i.value.clone()).ok_or(
                TranslateError::UnreachableForeignKeyColumns(
                    columns.iter().map(ToString::to_string).collect::<String>(),
                ),
            )?;

            let referenced_column_name = referred_columns
                .first()
                .ok_or(TranslateError::UnreachableForeignKeyColumns(
                    columns.iter().map(ToString::to_string).collect::<String>(),
                ))?
                .value
                .clone();

            let referenced_table_name = translate_object_name(foreign_table)?;

            let name = match name {
                Some(name) => name.value.clone(),
                None => {
                    format!(
                        "FK_{referencing_column_name}-{referenced_table_name}_{referenced_column_name}"
                    )
                }
            };

            Ok(ForeignKey {
                name,
                referencing_column_name,
                referenced_table_name,
                referenced_column_name,
                on_delete: translate_referential_action(on_delete)?,
                on_update: translate_referential_action(on_update)?,
            })
        }
        _ => Err(TranslateError::UnsupportedConstraint(table_constraint.to_string()).into()),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::parse_sql::parse};

    fn assert_translate_error(sql: &str, error: TranslateError) {
        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));
        let expected = Err::<Statement, _>(error.into());
        assert_eq!(actual, expected);
    }

    fn assert_invalid_placeholder(params: &[ParamLiteral], placeholder: &str) {
        let err = bind_placeholder(params, placeholder).unwrap_err();
        assert_eq!(
            err,
            TranslateError::InvalidPlaceholder {
                placeholder: placeholder.to_owned(),
            }
            .into()
        );
    }

    #[test]
    fn statement() {
        assert_translate_error(
            "INSERT INTO Foo DEFAULT VALUES",
            TranslateError::DefaultValuesOnInsertNotSupported("Foo".to_owned()),
        );
    }

    #[test]
    fn test_tuple_assignment_on_update_not_supported() {
        assert_translate_error(
            "UPDATE Foo SET (a, b) = (1, 2)",
            TranslateError::TupleAssignmentOnUpdateNotSupported("(a, b) = (1, 2)".to_owned()),
        );
    }

    #[test]
    fn insert_options_not_supported() {
        let cases = [
            (
                "INSERT INTO Foo VALUES (1) RETURNING *",
                TranslateError::UnsupportedInsertOption("RETURNING clause"),
            ),
            (
                "INSERT INTO Foo VALUES (1) ON CONFLICT DO NOTHING",
                TranslateError::UnsupportedInsertOption("ON CONFLICT clause"),
            ),
            (
                "INSERT INTO Foo AS f VALUES (1)",
                TranslateError::UnsupportedInsertOption("table alias"),
            ),
            (
                "INSERT INTO Foo PARTITION (bar = 1) VALUES (1)",
                TranslateError::UnsupportedInsertOption("PARTITION clause"),
            ),
            (
                "INSERT OVERWRITE TABLE Foo VALUES (1)",
                TranslateError::UnsupportedInsertOption("OVERWRITE clause"),
            ),
            (
                "INSERT TABLE Foo VALUES (1)",
                TranslateError::UnsupportedInsertOption("TABLE keyword"),
            ),
        ];

        for (sql, err) in cases {
            assert_translate_error(sql, err);
        }
    }

    #[test]
    fn update_options_not_supported() {
        let cases = [
            (
                "UPDATE Foo SET id = 1 FROM Bar",
                TranslateError::UnsupportedUpdateOption("FROM clause"),
            ),
            (
                "UPDATE Foo SET id = 1 WHERE id = 1 RETURNING *",
                TranslateError::UnsupportedUpdateOption("RETURNING clause"),
            ),
        ];

        for (sql, err) in cases {
            assert_translate_error(sql, err);
        }
    }

    #[test]
    fn delete_options_not_supported() {
        let cases = [
            (
                "DELETE FROM Foo USING Bar",
                TranslateError::UnsupportedDeleteOption("USING clause"),
            ),
            (
                "DELETE FROM Foo WHERE id = 1 RETURNING *",
                TranslateError::UnsupportedDeleteOption("RETURNING clause"),
            ),
            (
                "DELETE FROM Foo WHERE id = 1 ORDER BY id",
                TranslateError::UnsupportedDeleteOption("ORDER BY clause"),
            ),
            (
                "DELETE FROM Foo WHERE id = 1 LIMIT 1",
                TranslateError::UnsupportedDeleteOption("LIMIT clause"),
            ),
        ];

        for (sql, err) in cases {
            assert_translate_error(sql, err);
        }
    }

    #[test]
    fn reject_non_dollar_placeholder() {
        assert_invalid_placeholder(&[], "?1");
    }

    #[test]
    fn reject_zero_index_placeholder() {
        let params = [1_i64.into_param_literal().unwrap()];
        assert_invalid_placeholder(&params, "$0");
    }

    #[test]
    fn reject_non_numeric_index_placeholder() {
        let params = [1_i64.into_param_literal().unwrap()];
        assert_invalid_placeholder(&params, "$foo");
    }
}
