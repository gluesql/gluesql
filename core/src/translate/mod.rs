mod ast_literal;
mod data_type;
mod ddl;
mod error;
mod expr;
mod function;
mod operator;
mod query;

use crate::ast::UniqueConstraint;
use itertools::Itertools;

pub use self::{
    data_type::translate_data_type,
    ddl::{translate_column_def, translate_operate_function_arg},
    error::TranslateError,
    expr::{translate_expr, translate_order_by_expr},
    query::{alias_or_name, translate_query, translate_select_item},
};

use {
    crate::{
        ast::{Assignment, ForeignKey, ReferentialAction, Statement, Variable},
        result::Result,
    },
    ddl::translate_alter_table_operation,
    sqlparser::ast::{
        Assignment as SqlAssignment, Delete as SqlDelete, FromTable as SqlFromTable,
        Ident as SqlIdent, Insert as SqlInsert, ObjectName as SqlObjectName,
        ObjectType as SqlObjectType, Statement as SqlStatement,
        TableConstraint as SqlTableConstraint, TableFactor, TableWithJoins,
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
            constraints,
            comment,
            ..
        } => {
            let mut unique_constraints = Vec::new();
            let mut primary_key: Option<Vec<usize>> = None;

            let translated_columns = columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let (translated_column, is_primary, is_unique) = translate_column_def(column)?;
                    if is_primary {
                        match primary_key.as_mut() {
                            Some(_) => {
                                return Err(TranslateError::MultiplePrimaryKeyNotSupported.into())
                            }
                            None => {
                                primary_key.get_or_insert_with(Vec::new).push(index);
                            }
                        }
                    }
                    if is_unique {
                        unique_constraints.push(UniqueConstraint::new(None, vec![index]));
                    }
                    Ok(translated_column)
                })
                .collect::<Result<Vec<_>>>()?;

            let mut foreign_keys = Vec::new();

            for constraint in constraints {
                if let sqlparser::ast::TableConstraint::PrimaryKey {
                    columns: primary_key_columns,
                    ..
                } = constraint
                {
                    match primary_key.as_mut() {
                        Some(_) => {
                            return Err(TranslateError::MultiplePrimaryKeyNotSupported.into())
                        }
                        None => {
                            for column in primary_key_columns {
                                primary_key.get_or_insert_with(Vec::new).push(
                                    translated_columns
                                        .iter()
                                        .position(|v| v.name == column.value)
                                        .ok_or_else(|| {
                                            TranslateError::ColumnNotFoundInTable(
                                                column.to_string(),
                                            )
                                        })?,
                                );
                            }
                        }
                    }
                } else if let sqlparser::ast::TableConstraint::Unique {
                    name,
                    columns: unique_columns,
                    ..
                } = constraint
                {
                    let mut indices = Vec::new();
                    for column in unique_columns {
                        if let Some(index) =
                            columns.iter().position(|c| c.name.value == column.value)
                        {
                            indices.push(index);
                        } else {
                            return Err(TranslateError::ColumnNotFoundInTable(
                                column.value.clone(),
                            )
                            .into());
                        }
                    }
                    unique_constraints.push(UniqueConstraint::new(
                        name.clone().map(|n| n.value),
                        indices,
                    ));
                } else {
                    foreign_keys.push(translate_foreign_key(constraint)?);
                }
            }

            if let Some(primary_key) = primary_key.as_ref() {
                if primary_key.is_empty() {
                    return Err(TranslateError::EmptyPrimaryKeyConstraint.into());
                }

                // Check whether there are repeated columns in primary key constraint
                let primary_key_columns = primary_key.iter().map(|i| &translated_columns[*i].name);

                if let Some(repeated_column) = primary_key_columns
                    .clone()
                    .find(|column| primary_key_columns.clone().filter(|c| c == column).count() > 1)
                {
                    return Err(TranslateError::RepeatedColumnsInPrimaryKeyConstraint(
                        repeated_column.clone(),
                    )
                    .into());
                }
            }

            for (index, unique_constraint) in unique_constraints.iter().enumerate() {
                let unique_columns = unique_constraint
                    .column_indices()
                    .iter()
                    .map(|i| &translated_columns[*i].name);

                // Check whether there are repeated columns in unique constraints
                if let Some(repeated_column) = unique_columns
                    .clone()
                    .find(|column| unique_columns.clone().filter(|c| c == column).count() > 1)
                {
                    return Err(TranslateError::RepeatedColumnsInUniqueConstraint(
                        repeated_column.clone(),
                    )
                    .into());
                }

                // Check whether there are duplicated unique constraints
                if unique_constraints
                    .iter()
                    .skip(index + 1)
                    .any(|c| c.column_indices() == unique_constraint.column_indices())
                {
                    return Err(TranslateError::DuplicatedUniqueConstraint(
                        unique_columns.clone().join(", "),
                    )
                    .into());
                }

                // Check whether there are empty unique constraints
                if unique_constraint.column_indices().is_empty() {
                    return Err(TranslateError::EmptyUniqueConstraintColumns.into());
                }

                // Check whether there are multiple named unique constraints
                if unique_constraint.name().is_some()
                    && unique_constraints
                        .iter()
                        .skip(index + 1)
                        .any(|c| c.name() == unique_constraint.name())
                {
                    return Err(TranslateError::DuplicatedUniqueConstraintName(
                        unique_constraint.name().unwrap().to_owned(),
                    )
                    .into());
                }
            }

            let translated_columns = (!translated_columns.is_empty()).then_some(translated_columns);

            Ok(Statement::CreateTable {
                if_not_exists: *if_not_exists,
                name: translate_object_name(name)?,
                columns: translated_columns,
                source: query
                    .as_ref()
                    .map(|query| translate_query(query).map(Box::new))
                    .transpose()?,
                engine: engine.clone(),
                foreign_keys,
                primary_key,
                unique_constraints,
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
                    columns.iter().map(|i| i.to_string()).collect::<String>(),
                ),
            )?;

            let referenced_column_name = referred_columns
                .first()
                .ok_or(TranslateError::UnreachableForeignKeyColumns(
                    columns.iter().map(|i| i.to_string()).collect::<String>(),
                ))?
                .value
                .clone();

            let referenced_table_name = translate_object_name(foreign_table)?;

            let name = match name {
                Some(name) => name.value.clone(),
                None => {
                    format!("FK_{referencing_column_name}-{referenced_table_name}_{referenced_column_name}")
                }
            };

            Ok(ForeignKey {
                name,
                referencing_column_name,
                referenced_table_name,
                referenced_column_name,
                on_delete: on_delete
                    .map(ReferentialAction::from)
                    .unwrap_or(ReferentialAction::NoAction),
                on_update: on_update
                    .map(ReferentialAction::from)
                    .unwrap_or(ReferentialAction::NoAction),
            })
        }
        _ => Err(TranslateError::UnsupportedConstraint(table_constraint.to_string()).into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{ColumnDef, DataType};
    use {super::*, crate::parse_sql::parse};

    #[test]
    fn statement() {
        let sql = "INSERT INTO Foo DEFAULT VALUES";
        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));
        let expected =
            Err(TranslateError::DefaultValuesOnInsertNotSupported("Foo".to_owned()).into());

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_on_update_cascade() {
        let sql = "CREATE TABLE Foo (id INTEGER PRIMARY KEY, bar INTEGER, FOREIGN KEY (bar) REFERENCES Foo (id) ON UPDATE CASCADE)";
        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));
        let expected = Ok(Statement::CreateTable {
            if_not_exists: false,
            name: "Foo".to_owned(),
            columns: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    comment: None,
                },
                ColumnDef {
                    name: "bar".to_owned(),
                    data_type: DataType::Int,
                    nullable: true,
                    default: None,
                    comment: None,
                },
            ]),
            source: None,
            engine: None,
            foreign_keys: vec![ForeignKey {
                name: "FK_bar-Foo_id".to_owned(),
                referencing_column_name: "bar".to_owned(),
                referenced_table_name: "Foo".to_owned(),
                referenced_column_name: "id".to_owned(),
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::Cascade,
            }],
            primary_key: Some(vec![0]),
            unique_constraints: vec![],
            comment: None,
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_table_with_unknown_primary_key() {
        let sql = "CREATE TABLE Foo (id INTEGER, PRIMARY KEY (unknown))";

        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));

        let expected = Err(TranslateError::ColumnNotFoundInTable("unknown".to_owned()).into());

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_on_delete_cascade() {
        let sql = "CREATE TABLE Foo (id INTEGER PRIMARY KEY, bar INTEGER, FOREIGN KEY (bar) REFERENCES Foo (id) ON DELETE CASCADE)";
        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));
        let expected = Ok(Statement::CreateTable {
            if_not_exists: false,
            name: "Foo".to_owned(),
            columns: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    comment: None,
                },
                ColumnDef {
                    name: "bar".to_owned(),
                    data_type: DataType::Int,
                    nullable: true,
                    default: None,
                    comment: None,
                },
            ]),
            source: None,
            engine: None,
            foreign_keys: vec![ForeignKey {
                name: "FK_bar-Foo_id".to_owned(),
                referencing_column_name: "bar".to_owned(),
                referenced_table_name: "Foo".to_owned(),
                referenced_column_name: "id".to_owned(),
                on_delete: ReferentialAction::Cascade,
                on_update: ReferentialAction::NoAction,
            }],
            primary_key: Some(vec![0]),
            unique_constraints: vec![],
            comment: None,
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_table_with_multiple_primary_key() {
        let sql = "CREATE TABLE Foo (id INTEGER PRIMARY KEY, PRIMARY KEY (id, id2))";

        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));

        let expected = Err(TranslateError::MultiplePrimaryKeyNotSupported.into());

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_on_delete_on_update_cascade_same_line() {
        let sql = "CREATE TABLE Foo (id INTEGER PRIMARY KEY, bar INTEGER, FOREIGN KEY (bar) REFERENCES Foo (id) ON DELETE CASCADE ON UPDATE CASCADE)";
        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));
        let expected = Ok(Statement::CreateTable {
            if_not_exists: false,
            name: "Foo".to_owned(),
            columns: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    comment: None,
                },
                ColumnDef {
                    name: "bar".to_owned(),
                    data_type: DataType::Int,
                    nullable: true,
                    default: None,
                    comment: None,
                },
            ]),
            source: None,
            engine: None,
            foreign_keys: vec![ForeignKey {
                name: "FK_bar-Foo_id".to_owned(),
                referencing_column_name: "bar".to_owned(),
                referenced_table_name: "Foo".to_owned(),
                referenced_column_name: "id".to_owned(),
                on_delete: ReferentialAction::Cascade,
                on_update: ReferentialAction::Cascade,
            }],
            primary_key: Some(vec![0]),
            unique_constraints: vec![],
            comment: None,
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_table_with_multiple_primary_key_columns() {
        let sql = "CREATE TABLE Foo (id INTEGER PRIMARY KEY, id2 INTEGER PRIMARY KEY)";

        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));

        let expected = Err(TranslateError::MultiplePrimaryKeyNotSupported.into());

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_table_with_repeated_unique_constraint_columns() {
        let sql = "CREATE TABLE Foo (id INTEGER, id2 INTEGER, UNIQUE (id, id))";

        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));

        let expected =
            Err(TranslateError::RepeatedColumnsInUniqueConstraint("id".to_owned()).into());

        assert_eq!(actual, expected);

        let sql = "CREATE TABLE Foo (id INTEGER UNIQUE, id2 INTEGER, UNIQUE (id))";
        let actual = parse(sql).and_then(|parsed| translate(&parsed[0]));

        let expected = Err(TranslateError::DuplicatedUniqueConstraint("id".to_owned()).into());

        assert_eq!(actual, expected);
    }
}
