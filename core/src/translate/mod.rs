mod ast_literal;
mod data_type;
mod ddl;
mod error;
mod expr;
mod function;
mod operator;
mod query;

pub use self::{
    error::TranslateError,
    expr::{translate_expr, translate_order_by_expr},
    query::translate_query,
};

#[cfg(feature = "alter-table")]
use ddl::translate_alter_table_operation;
use sqlparser::ast::{TableFactor, TableWithJoins};

#[cfg(feature = "metadata")]
use crate::ast::Variable;

use {
    self::ddl::translate_column_def,
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
            query,
            ..
        } => Ok(Statement::CreateTable {
            if_not_exists: *if_not_exists,
            name: translate_object_name(name),
            columns: columns
                .iter()
                .map(translate_column_def)
                .collect::<Result<_>>()?,
            source: match query {
                Some(v) => Some(translate_query(v).map(Box::new)?),
                None => None,
            },
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
        #[cfg(feature = "index")]
        SqlStatement::CreateIndex {
            name,
            table_name,
            columns,
            ..
        } => {
            if columns.len() > 1 {
                return Err(TranslateError::CompositeIndexNotSupported.into());
            }

            Ok(Statement::CreateIndex {
                name: translate_object_name(name),
                table_name: translate_object_name(table_name),
                column: translate_order_by_expr(&columns[0])?,
            })
        }
        #[cfg(feature = "index")]
        SqlStatement::Drop {
            object_type: SqlObjectType::Index,
            names,
            ..
        } => {
            if names.len() > 1 {
                return Err(TranslateError::TooManyParamsInDropIndex.into());
            }

            let object_name: &Vec<SqlIdent> = &names[0].0;

            let table_name = ObjectName(vec![object_name[0].value.to_owned()]);
            let name = ObjectName(vec![object_name[1].value.to_owned()]);

            Ok(Statement::DropIndex { name, table_name })
        }
        #[cfg(feature = "transaction")]
        SqlStatement::StartTransaction { .. } => Ok(Statement::StartTransaction),
        #[cfg(feature = "transaction")]
        SqlStatement::Commit { .. } => Ok(Statement::Commit),
        #[cfg(feature = "transaction")]
        SqlStatement::Rollback { .. } => Ok(Statement::Rollback),
        #[cfg(feature = "metadata")]
        SqlStatement::ShowVariable { variable } => match (variable.len(), variable.get(0)) {
            (1, Some(keyword)) => match keyword.value.to_uppercase().as_str() {
                "TABLES" => Ok(Statement::ShowVariable(Variable::Tables)),
                "VERSION" => Ok(Statement::ShowVariable(Variable::Version)),
                v => Err(TranslateError::UnsupportedShowVariableKeyword(v.to_string()).into()),
            },
            _ => Err(
                TranslateError::UnsupportedShowVariableStatement(sql_statement.to_string()).into(),
            ),
        },
        SqlStatement::ShowColumns {
           table_name, ..
        } => Ok(Statement::ShowColumns {
            table_name: translate_object_name(table_name),
        }),
        _ => Err(TranslateError::UnsupportedStatement(sql_statement.to_string()).into()),
    }
}

fn translate_assignment(sql_assignment: &SqlAssignment) -> Result<Assignment> {
    let SqlAssignment { id, value } = sql_assignment;

    if id.len() > 1 {
        return Err(
            TranslateError::CompoundIdentOnUpdateNotSupported(sql_assignment.to_string()).into(),
        );
    }

    Ok(Assignment {
        id: id
            .get(0)
            .ok_or(TranslateError::UnreachableEmptyIdent)?
            .value
            .to_owned(),
        value: translate_expr(value)?,
    })
}

fn translate_table_with_join(table: &TableWithJoins) -> Result<ObjectName> {
    if !table.joins.is_empty() {
        return Err(TranslateError::JoinOnUpdateNotSupported.into());
    }
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(translate_object_name(name)),
        t => Err(TranslateError::UnsupportedTableFactor(t.to_string()).into()),
    }
}

fn translate_object_name(sql_object_name: &SqlObjectName) -> ObjectName {
    ObjectName(translate_idents(&sql_object_name.0))
}

fn translate_idents(idents: &[SqlIdent]) -> Vec<String> {
    idents.iter().map(|v| v.value.to_owned()).collect()
}
