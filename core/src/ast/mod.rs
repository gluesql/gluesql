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

pub trait ToSql {
    fn to_sql(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Statement {
    ShowColumns {
        table_name: String,
    },
    /// SELECT, VALUES
    Query(Query),
    /// INSERT
    Insert {
        /// TABLE
        table_name: String,
        /// COLUMNS
        columns: Vec<String>,
        /// A SQL query that specifies what to insert
        source: Query,
    },
    /// UPDATE
    Update {
        /// TABLE
        table_name: String,
        /// Column assignments
        assignments: Vec<Assignment>,
        /// WHERE
        selection: Option<Expr>,
    },
    /// DELETE
    Delete {
        /// FROM
        table_name: String,
        /// WHERE
        selection: Option<Expr>,
    },
    /// CREATE TABLE
    CreateTable {
        if_not_exists: bool,
        /// Table name
        name: String,
        /// Optional schema
        columns: Vec<ColumnDef>,
        source: Option<Box<Query>>,
    },
    /// ALTER TABLE
    #[cfg(feature = "alter-table")]
    AlterTable {
        /// Table name
        name: String,
        operation: AlterTableOperation,
    },
    /// DROP TABLE
    DropTable {
        /// An optional `IF EXISTS` clause. (Non-standard.)
        if_exists: bool,
        /// One or more objects to drop. (ANSI SQL requires exactly one.)
        names: Vec<String>,
    },
    /// CREATE INDEX
    #[cfg(feature = "index")]
    CreateIndex {
        name: String,
        table_name: String,
        column: OrderByExpr,
    },
    /// DROP INDEX
    #[cfg(feature = "index")]
    DropIndex {
        name: String,
        table_name: String,
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
    ShowVariable(Variable),
    #[cfg(feature = "index")]
    ShowIndexes(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Assignment {
    pub id: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Variable {
    Tables,
    Version,
}

impl ToSql for Statement {
    fn to_sql(&self) -> String {
        match self {
            Statement::ShowColumns { table_name } => {
                format!("SHOW COLUMNS FROM {table_name};")
            }
            Statement::Insert {
                table_name,
                columns,
                source,
            } => {
                let columns = match columns.is_empty() {
                    true => "".to_owned(),
                    false => format!("({}) ", columns.join(", ")),
                };

                format!("INSERT INTO {table_name} {columns}{};", source.to_sql())
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
                            "UPDATE {table_name} SET {assignments} WHERE {};",
                            expr.to_sql()
                        )
                    }
                    None => format!("UPDATE {table_name} SET {assignments};"),
                }
            }
            Statement::Delete {
                table_name,
                selection,
            } => match selection {
                Some(expr) => format!("DELETE FROM {table_name} WHERE {};", expr.to_sql()),
                None => format!("DELETE FROM {table_name};"),
            },
            Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                source,
            } => match source {
                Some(query) => match if_not_exists {
                    true => format!("CREATE TABLE IF NOT EXISTS {name} AS {};", query.to_sql()),
                    false => format!("CREATE TABLE {name} AS {};", query.to_sql()),
                },
                None => {
                    let columns = columns
                        .iter()
                        .map(ToSql::to_sql)
                        .collect::<Vec<_>>()
                        .join(", ");
                    match if_not_exists {
                        true => format!("CREATE TABLE IF NOT EXISTS {name} ({columns});"),
                        false => format!("CREATE TABLE {name} ({columns});"),
                    }
                }
            },
            #[cfg(feature = "alter-table")]
            Statement::AlterTable { name, operation } => {
                format!("ALTER TABLE {name} {};", operation.to_sql())
            }
            Statement::DropTable { if_exists, names } => {
                let names = names.join(", ");
                match if_exists {
                    true => format!("DROP TABLE IF EXISTS {};", names),
                    false => format!("DROP TABLE {};", names),
                }
            }
            #[cfg(feature = "index")]
            Statement::CreateIndex {
                name,
                table_name,
                column,
            } => {
                format!("CREATE INDEX {name} ON {table_name} {};", column.to_sql())
            }
            #[cfg(feature = "index")]
            Statement::DropIndex { name, table_name } => {
                format!("DROP INDEX {table_name}.{name};")
            }
            #[cfg(feature = "transaction")]
            Statement::StartTransaction => "START TRANSACTION;".to_owned(),
            #[cfg(feature = "transaction")]
            Statement::Commit => "COMMIT;".to_owned(),
            #[cfg(feature = "transaction")]
            Statement::Rollback => "ROLLBACK;".to_owned(),
            Statement::ShowVariable(variable) => match variable {
                Variable::Tables => "SHOW TABLES;".to_owned(),
                Variable::Version => "SHOW VERSIONS;".to_owned(),
            },
            #[cfg(feature = "index")]
            Statement::ShowIndexes(object_name) => {
                format!("SHOW INDEXES FROM {object_name};")
            }
            _ => "(..statement..)".to_owned(),
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

    #[cfg(feature = "index")]
    use crate::ast::OrderByExpr;

    use {
        crate::ast::{
            Assignment, AstLiteral, BinaryOperator, ColumnDef, ColumnOption, ColumnOptionDef,
            DataType, Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
            TableWithJoins, ToSql, Values, Variable,
        },
        bigdecimal::BigDecimal,
        std::str::FromStr,
    };

    #[test]
    fn to_sql_show_columns() {
        assert_eq!(
            "SHOW COLUMNS FROM Bar;",
            Statement::ShowColumns {
                table_name: "Bar".into()
            }
            .to_sql()
        )
    }

    #[test]
    fn to_sql_insert() {
        assert_eq!(
            "INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello');",
            Statement::Insert {
                table_name: "Test".into(),
                columns: vec!["id".to_owned(), "num".to_owned(), "name".to_owned()],
                source: Query {
                    body: SetExpr::Values(Values(vec![vec![
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1").unwrap())),
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap())),
                        Expr::Literal(AstLiteral::QuotedString("Hello".to_owned()))
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
            "UPDATE Foo SET id = 4, color = 'blue';",
            Statement::Update {
                table_name: "Foo".into(),
                assignments: vec![
                    Assignment {
                        id: "id".to_owned(),
                        value: Expr::Literal(AstLiteral::Number(
                            BigDecimal::from_str("4").unwrap()
                        ))
                    },
                    Assignment {
                        id: "color".to_owned(),
                        value: Expr::Literal(AstLiteral::QuotedString("blue".to_owned()))
                    }
                ],
                selection: None
            }
            .to_sql()
        );

        assert_eq!(
            "UPDATE Foo SET name = 'first' WHERE a > b;",
            Statement::Update {
                table_name: "Foo".into(),
                assignments: vec![Assignment {
                    id: "name".to_owned(),
                    value: Expr::Literal(AstLiteral::QuotedString("first".to_owned()))
                }],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("a".to_owned())),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Identifier("b".to_owned()))
                })
            }
            .to_sql()
        )
    }

    #[test]
    fn to_sql_delete() {
        assert_eq!(
            "DELETE FROM Foo;",
            Statement::Delete {
                table_name: "Foo".into(),
                selection: None
            }
            .to_sql()
        );

        assert_eq!(
            "DELETE FROM Foo WHERE item = 'glue';",
            Statement::Delete {
                table_name: "Foo".into(),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("item".to_owned())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Literal(AstLiteral::QuotedString("glue".to_owned())))
                })
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_create_table() {
        assert_eq!(
            "CREATE TABLE IF NOT EXISTS Foo ();",
            Statement::CreateTable {
                if_not_exists: true,
                name: "Foo".into(),
                columns: vec![],
                source: None
            }
            .to_sql()
        );

        assert_eq!(
            "CREATE TABLE Foo (id INT, num INT NULL, name TEXT);",
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        data_type: DataType::Int,
                        options: vec![]
                    },
                    ColumnDef {
                        name: "num".to_owned(),
                        data_type: DataType::Int,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null
                        }]
                    },
                    ColumnDef {
                        name: "name".to_owned(),
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
            "CREATE TABLE Foo AS SELECT id, count FROM Bar;",
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: vec![],
                source: Some(Box::new(Query {
                    body: SetExpr::Select(Box::new(Select {
                        projection: vec![
                            SelectItem::Expr {
                                expr: Expr::Identifier("id".to_owned()),
                                label: "".to_owned()
                            },
                            SelectItem::Expr {
                                expr: Expr::Identifier("count".to_owned()),
                                label: "".to_owned()
                            }
                        ],
                        from: TableWithJoins {
                            relation: TableFactor::Table {
                                name: "Bar".to_owned(),
                                alias: None,
                                index: None
                            },
                            joins: vec![]
                        },
                        selection: None,
                        group_by: vec![],
                        having: None
                    })),
                    order_by: vec![],
                    limit: None,
                    offset: None
                }))
            }
            .to_sql()
        );

        assert_eq!(
            "CREATE TABLE IF NOT EXISTS Foo AS VALUES (TRUE);",
            Statement::CreateTable {
                if_not_exists: true,
                name: "Foo".into(),
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
            "ALTER TABLE Foo ADD COLUMN amount INT DEFAULT 10;",
            Statement::AlterTable {
                name: "Foo".into(),
                operation: AlterTableOperation::AddColumn {
                    column_def: ColumnDef {
                        name: "amount".to_owned(),
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
            "ALTER TABLE Foo DROP COLUMN something;",
            Statement::AlterTable {
                name: "Foo".into(),
                operation: AlterTableOperation::DropColumn {
                    column_name: "something".to_owned(),
                    if_exists: false
                }
            }
            .to_sql()
        );

        assert_eq!(
            "ALTER TABLE Foo DROP COLUMN IF EXISTS something;",
            Statement::AlterTable {
                name: "Foo".into(),
                operation: AlterTableOperation::DropColumn {
                    column_name: "something".to_owned(),
                    if_exists: true
                }
            }
            .to_sql()
        );

        assert_eq!(
            "ALTER TABLE Bar RENAME COLUMN id TO new_id;",
            Statement::AlterTable {
                name: "Bar".into(),
                operation: AlterTableOperation::RenameColumn {
                    old_column_name: "id".to_owned(),
                    new_column_name: "new_id".to_owned()
                }
            }
            .to_sql()
        );

        assert_eq!(
            "ALTER TABLE Foo RENAME TO Bar;",
            Statement::AlterTable {
                name: "Foo".to_owned(),
                operation: AlterTableOperation::RenameTable {
                    table_name: "Bar".to_owned(),
                }
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_drop_table() {
        assert_eq!(
            "DROP TABLE Test;",
            Statement::DropTable {
                if_exists: false,
                names: vec!["Test".into()]
            }
            .to_sql()
        );

        assert_eq!(
            "DROP TABLE IF EXISTS Test;",
            Statement::DropTable {
                if_exists: true,
                names: vec!["Test".into()]
            }
            .to_sql()
        );

        assert_eq!(
            "DROP TABLE Foo, Bar;",
            Statement::DropTable {
                if_exists: false,
                names: vec!["Foo".into(), "Bar".into(),]
            }
            .to_sql()
        );
    }

    #[test]
    #[cfg(feature = "index")]
    fn to_sql_create_index() {
        assert_eq!(
            "CREATE INDEX idx_name ON Test LastName;",
            Statement::CreateIndex {
                name: "idx_name".into(),
                table_name: "Test".into(),
                column: OrderByExpr {
                    expr: Expr::Identifier("LastName".to_owned()),
                    asc: None
                }
            }
            .to_sql()
        );
    }

    #[test]
    #[cfg(feature = "index")]
    fn to_sql_drop_index() {
        assert_eq!(
            "DROP INDEX Test.idx_id;",
            Statement::DropIndex {
                name: "idx_id".into(),
                table_name: "Test".into(),
            }
            .to_sql()
        )
    }

    #[test]
    #[cfg(feature = "transaction")]
    fn to_sql_transaction() {
        assert_eq!("START TRANSACTION;", Statement::StartTransaction.to_sql());
        assert_eq!("COMMIT;", Statement::Commit.to_sql());
        assert_eq!("ROLLBACK;", Statement::Rollback.to_sql());
    }

    #[test]
    fn to_sql_show_variable() {
        assert_eq!(
            "SHOW TABLES;",
            Statement::ShowVariable(Variable::Tables).to_sql()
        );
        assert_eq!(
            "SHOW VERSIONS;",
            Statement::ShowVariable(Variable::Version).to_sql()
        );
    }

    #[test]
    #[cfg(feature = "index")]
    fn to_sql_show_indexes() {
        assert_eq!(
            "SHOW INDEXES FROM Test;",
            Statement::ShowIndexes("Test".into()).to_sql()
        );
    }

    #[test]
    fn to_sql_assignment() {
        assert_eq!(
            "count = 5",
            Assignment {
                id: "count".to_owned(),
                value: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("5").unwrap()))
            }
            .to_sql()
        )
    }
}
