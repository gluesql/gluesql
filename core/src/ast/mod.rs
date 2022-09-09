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

impl ToSql for ObjectName {
    fn to_sql(&self) -> String {
        match self {
            ObjectName(names) => names.join("."),
        }
    }
}

impl ToSql for Statement {
    fn to_sql(&self) -> String {
        match self {
            Statement::Insert {
                table_name,
                columns,
                source: _,
            } => {
                let columns = columns.join(", ");
                format!(
                    "INSERT INTO {} ({columns}) (..query..)",
                    table_name.to_sql()
                )
            }
            Statement::Update {
                table_name,
                assignments,
                selection,
            } => {
                let assignments = assignments
                    .iter()
                    .map(ToSql::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ");
                match selection {
                    Some(expr) => {
                        format!(
                            "UPDATE {} SET {assignments} WHERE {}",
                            table_name.to_sql(),
                            expr.to_sql()
                        )
                    }
                    None => format!("UPDATE {} SET {assignments}", table_name.to_sql()),
                }
            }
            Statement::Delete {
                table_name,
                selection,
            } => match selection {
                Some(expr) => format!(
                    "DELETE FROM {} WHERE {}",
                    table_name.to_sql(),
                    expr.to_sql()
                ),
                None => format!("DELETE FROM {}", table_name.to_sql()),
            },
            Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                source,
            } => match source {
                Some(_query) => match if_not_exists {
                    true => format!(
                        "CREATE TABLE IF NOT EXISTS {} AS (..query..)",
                        name.to_sql()
                    ),
                    false => format!("CREATE TABLE {} AS (..query..)", name.to_sql()),
                },
                None => {
                    let columns = columns
                        .iter()
                        .map(ToSql::to_sql)
                        .collect::<Vec<_>>()
                        .join(", ");
                    match if_not_exists {
                        true => format!("CREATE TABLE IF NOT EXISTS {} ({columns})", name.to_sql()),
                        false => format!("CREATE TABLE {} ({columns})", name.to_sql()),
                    }
                }
            },
            #[cfg(feature = "alter-table")]
            Statement::AlterTable { name, operation } => {
                format!("ALTER TABLE {} {}", name.to_sql(), operation.to_sql())
            }
            _ => "(..statement..)".to_string(),
        }
    }
}

impl ToSql for Assignment {
    fn to_sql(&self) -> String {
        format!("{} = {}", self.id, self.value.to_sql())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "alter-table")]
    use crate::ast::AlterTableOperation;

    use crate::ast::Variable::{Tables, Version};
    use {
        crate::ast::{
            Assignment, AstLiteral, BinaryOperator, ColumnDef, ColumnOption, ColumnOptionDef,
            DataType, Expr, ObjectName, OrderByExpr, Query, SetExpr, Statement, ToSql, Values,
        },
        bigdecimal::BigDecimal,
        std::str::FromStr,
    };

    #[test]
    fn to_sql_object_name() {
        assert_eq!("Foo", ObjectName(vec!["Foo".to_string()]).to_sql());

        assert_eq!(
            "Foo.bar.bax",
            ObjectName(vec![
                "Foo".to_string(),
                "bar".to_string(),
                "bax".to_string()
            ])
            .to_sql()
        );
    }

    #[test]
    fn to_sql_show_columns() {
        assert_eq!(
            "SHOW COLUMNS from Bar",
            Statement::ShowColumns {
                table_name: ObjectName(vec!["Bar".to_string()])
            }
            .to_sql()
        )
    }

    #[test]
    fn to_sql_insert() {
        assert_eq!(
            "INSERT INTO Test (id, num, name) (..query..)",
            Statement::Insert {
                table_name: ObjectName(vec!["Test".to_string()]),
                columns: vec!["id".to_string(), "num".to_string(), "name".to_string()],
                source: Query {
                    body: SetExpr::Values(Values(vec![vec![
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1").unwrap())),
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap())),
                        Expr::Literal(AstLiteral::QuotedString("Hello".to_string()))
                    ]])),
                    order_by: vec![],
                    limit: None,
                    offset: None
                }
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_update() {
        assert_eq!(
            r#"UPDATE Foo SET id = 4, color = "blue""#,
            Statement::Update {
                table_name: ObjectName(vec!["Foo".to_string()]),
                assignments: vec![
                    Assignment {
                        id: "id".to_string(),
                        value: Expr::Literal(AstLiteral::Number(
                            BigDecimal::from_str("4").unwrap()
                        ))
                    },
                    Assignment {
                        id: "color".to_string(),
                        value: Expr::Literal(AstLiteral::QuotedString("blue".to_string()))
                    }
                ],
                selection: None
            }
            .to_sql()
        );

        assert_eq!(
            r#"UPDATE Foo SET name = "first" WHERE a > b"#,
            Statement::Update {
                table_name: ObjectName(vec!["Foo".to_string()]),
                assignments: vec![Assignment {
                    id: "name".to_string(),
                    value: Expr::Literal(AstLiteral::QuotedString("first".to_string()))
                }],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("a".to_string())),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Identifier("b".to_string()))
                })
            }
            .to_sql()
        )
    }

    #[test]
    fn to_sql_delete() {
        assert_eq!(
            "DELETE FROM Foo",
            Statement::Delete {
                table_name: ObjectName(vec!["Foo".to_string()]),
                selection: None
            }
            .to_sql()
        );

        assert_eq!(
            r#"DELETE FROM Foo WHERE item = "glue""#,
            Statement::Delete {
                table_name: ObjectName(vec!["Foo".to_string()]),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("item".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Literal(AstLiteral::QuotedString("glue".to_string())))
                })
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_create_table() {
        assert_eq!(
            "CREATE TABLE IF NOT EXISTS Foo ()",
            Statement::CreateTable {
                if_not_exists: true,
                name: ObjectName(vec!["Foo".to_string()]),
                columns: vec![],
                source: None
            }
            .to_sql()
        );

        assert_eq!(
            "CREATE TABLE Foo (id INT, num INT NULL, name TEXT)",
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

    #[test]
    fn to_sql_create_table_as() {
        assert_eq!(
            "CREATE TABLE Foo AS (..query..)",
            Statement::CreateTable {
                if_not_exists: false,
                name: ObjectName(vec!["Foo".to_string()]),
                columns: vec![],
                source: Some(Box::new(Query {
                    body: SetExpr::Values(Values(vec![vec![Expr::Literal(AstLiteral::Boolean(
                        false
                    ))]])),
                    order_by: vec![],
                    limit: None,
                    offset: None
                }))
            }
            .to_sql()
        );

        assert_eq!(
            "CREATE TABLE IF NOT EXISTS Foo AS (..query..)",
            Statement::CreateTable {
                if_not_exists: true,
                name: ObjectName(vec!["Foo".to_string()]),
                columns: vec![],
                source: Some(Box::new(Query {
                    body: SetExpr::Values(Values(vec![vec![Expr::Literal(AstLiteral::Boolean(
                        true
                    ))]])),
                    order_by: vec![],
                    limit: None,
                    offset: None
                }))
            }
            .to_sql()
        );
    }

    #[test]
    #[cfg(feature = "alter-table")]
    fn to_sql_alter_table() {
        assert_eq!(
            "ALTER TABLE Foo ADD COLUMN amount INT DEFAULT 10",
            Statement::AlterTable {
                name: ObjectName(vec!["Foo".to_string()]),
                operation: AlterTableOperation::AddColumn {
                    column_def: ColumnDef {
                        name: "amount".to_string(),
                        data_type: DataType::Int,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Default(Expr::Literal(AstLiteral::Number(
                                BigDecimal::from_str("10").unwrap()
                            )))
                        }]
                    }
                }
            }
            .to_sql()
        );

        assert_eq!(
            "ALTER TABLE Foo DROP COLUMN something",
            Statement::AlterTable {
                name: ObjectName(vec!["Foo".to_string()]),
                operation: AlterTableOperation::DropColumn {
                    column_name: "something".to_string(),
                    if_exists: false
                }
            }
            .to_sql()
        );

        assert_eq!(
            "ALTER TABLE Foo DROP COLUMN IF EXISTS something",
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
            "ALTER TABLE Foo RENAME TO Bar",
            Statement::AlterTable {
                name: ObjectName(vec!["Foo".to_string()]),
                operation: AlterTableOperation::RenameTable {
                    table_name: ObjectName(vec!["Bar".to_string()])
                }
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_drop_table() {
        assert_eq!(
            "DROP TABLE Test",
            Statement::DropTable {
                if_exists: false,
                names: vec![ObjectName(vec!["Test".to_string()])]
            }
            .to_sql()
        );

        assert_eq!(
            "DROP TABLE IF EXISTS Test",
            Statement::DropTable {
                if_exists: true,
                names: vec![ObjectName(vec!["Test".to_string()])]
            }
            .to_sql()
        )
    }

    #[test]
    fn to_sql_create_index() {
        assert_eq!(
            "CREATE INDEX idx_name ON Test (name)",
            Statement::CreateIndex {
                name: ObjectName(vec!["idx_name".to_string()]),
                table_name: ObjectName(vec!["Test".to_string()]),
                column: OrderByExpr {
                    expr: Expr::Identifier("LastName".to_string()),
                    asc: None
                }
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_drop_index() {
        assert_eq!(
            "DROP INDEX Test.idx_id",
            Statement::DropIndex {
                name: ObjectName(vec!["idx_id".to_string()]),
                table_name: ObjectName(vec!["Test".to_string()])
            }
            .to_sql()
        )
    }

    #[test]
    #[cfg(feature = "transaction")]
    fn to_sql_transaction() {
        assert_eq!("START TRANSACTION", Statement::StartTransaction.to_sql());
        assert_eq!("COMMIT", Statement::Commit.to_sql());
        assert_eq!("ROLLBACK", Statement::Rollback.to_sql());
    }

    #[test]
    #[cfg(feature = "metadata")]
    fn to_sql_show_variable() {
        assert_eq!("SHOW TABLES", Statement::ShowVariable(Tables).to_sql());
        assert_eq!("SHOW VERSIONS", Statement::ShowVariable(Version).to_sql());
    }

    #[test]
    #[cfg(feature = "index")]
    fn to_sql_show_indexes() {
        assert_eq!(
            "SHOW INDEXES from Test",
            Statement::ShowIndexes(ObjectName(vec!["Test".to_string()])).to_sql()
        );
    }

    #[test]
    fn to_sql_assignment() {
        assert_eq!(
            "count = 5",
            Assignment {
                id: "count".to_string(),
                value: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("5").unwrap()))
            }
            .to_sql()
        )
    }
}
