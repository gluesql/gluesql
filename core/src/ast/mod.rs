mod ast_literal;
mod data_type;
mod ddl;
mod expr;
mod function;
mod operator;
mod query;

pub use ast_literal::{AstLiteral, DateTimeField, TrimWhereField};
pub use data_type::DataType;
pub use ddl::*;
pub use expr::Expr;
pub use function::{Aggregate, CountArgExpr, Function};
pub use operator::*;
pub use query::*;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectName(pub Vec<String>);

pub trait ToSql {
    fn to_sql(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Statement {
    ShowColumns {
        table_name: ObjectName,
    },
    /// SELECT, VALUES
    Query(Query),
    /// INSERT
    Insert {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<String>,
        /// A SQL query that specifies what to insert
        source: Query,
    },
    /// UPDATE
    Update {
        /// TABLE
        table_name: ObjectName,
        /// Column assignments
        assignments: Vec<Assignment>,
        /// WHERE
        selection: Option<Expr>,
    },
    /// DELETE
    Delete {
        /// FROM
        table_name: ObjectName,
        /// WHERE
        selection: Option<Expr>,
    },
    /// CREATE TABLE
    CreateTable {
        if_not_exists: bool,
        /// Table name
        name: ObjectName,
        /// Optional schema
        columns: Vec<ColumnDef>,
        source: Option<Box<Query>>,
    },
    /// ALTER TABLE
    #[cfg(feature = "alter-table")]
    AlterTable {
        /// Table name
        name: ObjectName,
        operation: AlterTableOperation,
    },
    /// DROP TABLE
    DropTable {
        /// An optional `IF EXISTS` clause. (Non-standard.)
        if_exists: bool,
        /// One or more objects to drop. (ANSI SQL requires exactly one.)
        names: Vec<ObjectName>,
    },
    /// CREATE INDEX
    #[cfg(feature = "index")]
    CreateIndex {
        name: ObjectName,
        table_name: ObjectName,
        column: OrderByExpr,
    },
    /// DROP INDEX
    #[cfg(feature = "index")]
    DropIndex {
        name: ObjectName,
        table_name: ObjectName,
    },
    /// START TRANSACTION, BEGIN
    #[cfg(feature = "transaction")]
    StartTransaction,
    /// COMMIT
    #[cfg(feature = "transaction")]
    Commit,
    /// ROLLBACK
    #[cfg(feature = "transaction")]
    Rollback,
    /// SHOW VARIABLE
    #[cfg(feature = "metadata")]
    ShowVariable(Variable),
    #[cfg(feature = "index")]
    ShowIndexes(ObjectName),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Assignment {
    pub id: String,
    pub value: Expr,
}

#[cfg(feature = "metadata")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Variable {
    Tables,
    Version,
}

impl ToSql for Statement {
    fn to_sql(&self) -> String {
        match self {
            Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                source,
            } => match source {
                // TODO refactor
                // 1. 너무 nested... {}{}{}{}
                // 2. columns empty 일때를
                Some(query) => format!("CREATE TABLE {name} AS (..query..)"),
                None => {
                    if columns.is_empty() {
                        match if_not_exists {
                            true => format!("CREATE TABLE IF NOT EXISTS {name}"),
                            false => format!("CREATE TABLE {name}"),
                        }
                    } else {
                        let columns = columns
                            .iter()
                            .map(ToSql::to_sql)
                            .collect::<Vec<_>>()
                            .join(", ");
                        match if_not_exists {
                            true => format!("CREATE TABLE IF NOT EXISTS {name} ({columns})"),
                            false => format!("CREATE TABLE {name} ({columns})"),
                        }
                    }
                }
            },
            Statement::AlterTable { name, operation } => {
                let operation = operation.to_sql();
                format!("ALTER TABLE {name} {operation}")
            }
            _ => "(..statement..)",
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::AstLiteral::Number;
    use crate::ast::Expr::Literal;
    use crate::ast::Statement::Query;
    use crate::ast::{
        AlterTableOperation, ColumnDef, ColumnOption, ColumnOptionDef, DataType, ObjectName,
        Statement, ToSql,
    };
    use bigdecimal::BigDecimal;
    use regex::Regex;
    use std::str::FromStr;

    #[test]
    fn to_sql_create_table() {
        assert_eq!(
            "CREATE TABLE IF NOT EXISTS Foo",
            Statement::CreateTable {
                if_not_exists: true,
                name: ObjectName(vec!["Foo".to_string()]),
                columns: vec![],
                source: None
            }
            .to_sql()
        );

        assert_eq!(
            "CREATE TABLE Foo (id INTEGER, num INTEGER NULL, name TEXT)",
            Statement::CreateTable {
                if_not_exists: false,
                name: ObjectName(vec!["Foo".to_string()]),
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int,
                        options: vec![]
                    },
                    ColumnDef {
                        name: "num".to_string(),
                        data_type: DataType::Int,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null
                        }]
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        options: vec![]
                    }
                ],
                source: None
            }
            .to_sql()
        );
    }

    fn to_sql_alter_table() {
        assert_eq!(
            "ALTER TABLE Foo ADD COLUMN amount INTEGER DEFAULT 10",
            Statement::AlterTable {
                name: ObjectName(vec!["Foo".to_string()]),
                operation: AlterTableOperation::AddColumn {
                    column_def: ColumnDef {
                        name: "amount".to_string(),
                        data_type: DataType::Int,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Default(Literal(Number(
                                BigDecimal::from_str("10").unwrap()
                            )))
                        }]
                    }
                }
            }
            .to_sql()
        );

        assert_eq!(
            "ALTER TABLE Foo DROP COLUMN IF EXISTS something;",
            Statement::AlterTable {
                name: ObjectName(vec!["Foo".to_string()]),
                operation: AlterTableOperation::DropColumn {
                    column_name: "something".to_string(),
                    if_exists: true
                }
            }
            .to_sql()
        );

        assert_eq!(
            "ALTER TABLE Bar RENAME COLUMN id TO new_id",
            Statement::AlterTable {
                name: ObjectName(vec!["Bar".to_string()]),
                operation: AlterTableOperation::RenameColumn {
                    old_column_name: "id".to_string(),
                    new_column_name: "new_id".to_string()
                }
            }
            .to_sql()
        );

        assert_eq!(
            "ALTER TABLE Foo RENAME TO Bar;",
            Statement::AlterTable {
                name: ObjectName(vec!["Foo".to_string()]),
                operation: AlterTableOperation::RenameTable {
                    table_name: ObjectName(vec!["Bar".to_string()])
                }
            }
            .to_sql()
        );
    }
}
