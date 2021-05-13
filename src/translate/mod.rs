mod ast_literal;
mod data_type;
mod ddl;
mod error;
mod expr;
mod function;
mod operator;
mod query;

pub use error::TranslateError;

#[cfg(feature = "alter-table")]
use ddl::translate_alter_table_operation;

use {
    self::{ddl::translate_column_def, expr::translate_expr, query::translate_query},
    crate::{
        ast::{Assignment, ObjectName, Statement},
        result::Result,
    },
    sqlparser::ast::{
        Assignment as SqlAssignment, Ident as SqlIdent, ObjectName as SqlObjectName,
        ObjectType as SqlObjectType, Statement as SqlStatement,
    },
};

pub fn translate(sql_statement: &SqlStatement) -> Result<Statement> {
    match sql_statement {
        SqlStatement::Query(query) => translate_query(query).map(Box::new).map(Statement::Query),
        SqlStatement::Insert {
            table_name,
            columns,
            source,
            ..
        } => Ok(Statement::Insert {
            table_name: translate_object_name(table_name),
            columns: translate_idents(columns),
            source: translate_query(source).map(Box::new)?,
        }),
        SqlStatement::Update {
            table_name,
            assignments,
            selection,
            ..
        } => Ok(Statement::Update {
            table_name: translate_object_name(table_name),
            assignments: assignments
                .iter()
                .map(translate_assignment)
                .collect::<Result<_>>()?,
            selection: selection.as_ref().map(translate_expr).transpose()?,
        }),
        SqlStatement::Delete {
            table_name,
            selection,
        } => Ok(Statement::Delete {
            table_name: translate_object_name(table_name),
            selection: selection.as_ref().map(translate_expr).transpose()?,
        }),
        SqlStatement::CreateTable {
            if_not_exists,
            name,
            columns,
            ..
        } => Ok(Statement::CreateTable {
            if_not_exists: *if_not_exists,
            name: translate_object_name(name),
            columns: columns
                .iter()
                .map(translate_column_def)
                .collect::<Result<_>>()?,
        }),
        #[cfg(feature = "alter-table")]
        SqlStatement::AlterTable {
            name, operation, ..
        } => Ok(Statement::AlterTable {
            name: translate_object_name(name),
            operation: translate_alter_table_operation(operation)?,
        }),
        SqlStatement::Drop {
            object_type: SqlObjectType::Table,
            if_exists,
            names,
            ..
        } => Ok(Statement::DropTable {
            if_exists: *if_exists,
            names: names.iter().map(translate_object_name).collect(),
        }),
        _ => Err(TranslateError::UnsupportedStatement(sql_statement.to_string()).into()),
    }
}

fn translate_assignment(sql_assignment: &SqlAssignment) -> Result<Assignment> {
    let SqlAssignment { id, value } = sql_assignment;

    Ok(Assignment {
        id: id.value.to_owned(),
        value: translate_expr(value)?,
    })
}

fn translate_object_name(sql_object_name: &SqlObjectName) -> ObjectName {
    ObjectName(translate_idents(&sql_object_name.0))
}

fn translate_idents(idents: &[SqlIdent]) -> Vec<String> {
    idents.iter().map(|v| v.value.to_owned()).collect()
}
