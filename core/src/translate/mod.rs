mod ast_literal;
mod data_type;
mod ddl;
mod error;
mod expr;
mod function;
mod operator;
mod query;

pub use self::{
    data_type::translate_data_type,
    ddl::{translate_column_def, translate_operate_function_arg},
    error::TranslateError,
    expr::{translate_expr, translate_order_by_expr},
    query::{alias_or_name, translate_query, translate_select_item},
};

use {
    crate::{
        ast::{Assignment, Statement, Variable},
        result::Result,
    },
    ddl::translate_alter_table_operation,
    sqlparser::ast::{
        Assignment as SqlAssignment, Delete as SqlDelete, FromTable as SqlFromTable,
        Ident as SqlIdent, Insert as SqlInsert, ObjectName as SqlObjectName,
        ObjectType as SqlObjectType, Statement as SqlStatement, TableFactor, TableWithJoins,
    },
};

pub fn translate(sql_statement: &SqlStatement) -> Result<Statement> {
    match sql_statement {
        SqlStatement::Query(query) => translate_query(query).map(Statement::Query),
        SqlStatement::Insert(SqlInsert {
            table_name,
            columns,
            source,
            ..
        }) => {
            let table_name = translate_object_name(table_name)?;
            let columns = translate_idents(columns);
            let source = source
                .as_deref()
                .ok_or_else(|| {
                    TranslateError::DefaultValuesOnInsertNotSupported(table_name.clone()).into()
                })
                .and_then(translate_query)?;

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
            ..
        } => Ok(Statement::Update {
            table_name: translate_table_with_join(table)?,
            assignments: assignments
                .iter()
                .map(translate_assignment)
                .collect::<Result<_>>()?,
            selection: selection.as_ref().map(translate_expr).transpose()?,
        }),
        SqlStatement::Delete(SqlDelete {
            from, selection, ..
        }) => {
            let from = match from {
                SqlFromTable::WithFromKeyword(from) => from,
                SqlFromTable::WithoutKeyword(_) => {
                    return Err(TranslateError::UnreachableOmittingFromInDelete.into())
                }
            };
            let table_name = from
                .iter()
                .map(translate_table_with_join)
                .next()
                .ok_or(TranslateError::UnreachableEmptyTable)??;

            Ok(Statement::Delete {
                table_name,
                selection: selection.as_ref().map(translate_expr).transpose()?,
            })
        }
        SqlStatement::CreateTable {
            if_not_exists,
            name,
            columns,
            query,
            engine,
            comment,
            ..
        } => {
            let columns = columns
                .iter()
                .map(translate_column_def)
                .collect::<Result<Vec<_>>>()?;

            let columns = (!columns.is_empty()).then_some(columns);

            Ok(Statement::CreateTable {
                if_not_exists: *if_not_exists,
                name: translate_object_name(name)?,
                columns,
                source: match query {
                    Some(v) => Some(translate_query(v).map(Box::new)?),
                    None => None,
                },
                engine: engine.clone(),
                comment: comment.clone(),
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
                operation: translate_alter_table_operation(operation)?,
            })
        }
        SqlStatement::Drop {
            object_type: SqlObjectType::Table,
            if_exists,
            names,
            ..
        } => Ok(Statement::DropTable {
            if_exists: *if_exists,
            names: names
                .iter()
                .map(translate_object_name)
                .collect::<Result<Vec<_>>>()?,
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
        SqlStatement::CreateIndex {
            name,
            table_name,
            columns,
            ..
        } => {
            if columns.len() > 1 {
                return Err(TranslateError::CompositeIndexNotSupported.into());
            }

            let Some(name) = name else {
                return Err(TranslateError::UnsupportedUnnamedIndex.into());
            };

            let name = translate_object_name(name)?;

            if name.to_uppercase() == "PRIMARY" {
                return Err(TranslateError::ReservedIndexName(name).into());
            };

            Ok(Statement::CreateIndex {
                name,
                table_name: translate_object_name(table_name)?,
                column: translate_order_by_expr(&columns[0])?,
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

            let table_name = object_name[0].value.to_owned();
            let name = object_name[1].value.to_owned();

            if name.to_uppercase() == "PRIMARY" {
                return Err(TranslateError::CannotDropPrimary.into());
            };

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
                    Some(tablename) => Ok(Statement::ShowIndexes(tablename.value.to_owned())),
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
            params,
            ..
        } => {
            let args = args
                .as_ref()
                .map(|args| {
                    args.iter()
                        .map(translate_operate_function_arg)
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            Ok(Statement::CreateFunction {
                or_replace: *or_replace,
                name: translate_object_name(name)?,
                args: args.unwrap_or_default(),
                return_: params
                    .return_
                    .as_ref()
                    .map(translate_expr)
                    .transpose()?
                    .ok_or(TranslateError::UnsupportedEmptyFunctionBody)?,
            })
        }
        _ => Err(TranslateError::UnsupportedStatement(sql_statement.to_string()).into()),
    }
}

pub fn translate_assignment(sql_assignment: &SqlAssignment) -> Result<Assignment> {
    let SqlAssignment { id, value } = sql_assignment;

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
            .to_owned(),
        value: translate_expr(value)?,
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
        .map(|v| v.value.to_owned())
        .ok_or_else(|| TranslateError::UnreachableEmptyObject.into())
}

pub fn translate_idents(idents: &[SqlIdent]) -> Vec<String> {
    idents.iter().map(|v| v.value.to_owned()).collect()
}

#[cfg(test)]
mod tests {
    use {super::*, crate::parse_sql::parse};

    #[test]
    fn statement() {
        let sql = "INSERT INTO Foo DEFAULT VALUES";
        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));
        let expected =
            Err(TranslateError::DefaultValuesOnInsertNotSupported("Foo".to_owned()).into());

        assert_eq!(actual, expected);
    }
}
