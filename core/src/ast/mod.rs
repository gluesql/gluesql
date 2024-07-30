mod ast_literal;
mod data_type;
mod ddl;
mod expr;
mod function;
mod operator;
mod query;

pub use {
    ast_literal::{AstLiteral, DateTimeField, TrimWhereField},
    data_type::DataType,
    ddl::*,
    expr::Expr,
    function::{Aggregate, CountArgExpr, Function},
    operator::*,
    query::*,
};

use {
    serde::{Deserialize, Serialize},
    strum_macros::Display,
};

use itertools::Itertools;

pub trait ToSql {
    fn to_sql(&self) -> String;
}

pub trait ToSqlUnquoted {
    fn to_sql_unquoted(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Struct representing a UNIQUE constraint on a table.
///
/// A UNIQUE constraint is a single field or combination of fields that uniquely defines a record.
/// No two records can have the same value(s) for the field(s) defined by the UNIQUE constraint.
///
/// # Example
/// You can define a single-field UNIQUE constraint like this:
///
/// ```sql
/// CREATE TABLE Test (
///    id INTEGER,
///    name TEXT,
///    UNIQUE (id)
/// );
/// ```
///
/// Or, analogously, using explicit column constraints:
/// ```sql
/// CREATE TABLE Test (
///    id INTEGER,
///    name TEXT,
///    CONSTRAINT unique_id UNIQUE (id)
/// );
/// ```
///
/// Or a multi-field UNIQUE constraint like this:
/// ```sql
/// CREATE TABLE Test (
///     id INTEGER,
///     name TEXT,
///     UNIQUE (id, name)
/// );
/// ```
///
/// Or, analogously, using explicit column constraints:
/// ```sql
/// CREATE TABLE Test (
///    id INTEGER,
///    name TEXT,
///    CONSTRAINT unique_id_name UNIQUE (id, name)
/// );
/// ```
pub struct UniqueConstraint {
    /// Optional name for the constraint.
    name: Option<String>,
    /// The fields that are part of the UNIQUE constraint.
    columns: Vec<usize>,
}

impl UniqueConstraint {
    /// Creates a new UNIQUE constraint with the given name and columns.
    pub fn new(name: Option<String>, columns: Vec<usize>) -> Self {
        Self { name, columns }
    }

    /// Creates a new anonimous UNIQUE constraint with the given columns.
    pub fn new_anonimous(columns: Vec<usize>) -> Self {
        Self::new(None, columns)
    }

    /// Returns the name of the UNIQUE constraint.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the columns that are part of the UNIQUE constraint.
    pub fn column_indices(&self) -> &[usize] {
        &self.columns
    }

    /// Returns whether constraint includes the provided index.
    pub fn includes_column(&self, index: usize) -> bool {
        self.columns.contains(&index)
    }

    /// Returns whether it is composed of a single column, i.e. is a single-field UNIQUE constraint.
    pub fn is_single_field(&self) -> bool {
        self.columns.len() == 1
    }

    fn to_sql<S: AsRef<str>>(&self, column_names: &[S]) -> String {
        let columns = self
            .columns
            .iter()
            .map(|column| format!(r#""{}""#, column_names[*column].as_ref()))
            .join(", ");
        if let Some(name) = &self.name {
            format!(r#"CONSTRAINT "{}" UNIQUE ({})"#, name, columns)
        } else {
            format!("UNIQUE ({})", columns)
        }
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Hash, Serialize, Deserialize)]
pub struct ForeignKey {
    pub name: String,
    pub referencing_column_name: String,
    pub referenced_table_name: String,
    pub referenced_column_name: String,
    /// The action to perform when the referenced row is deleted.
    pub on_delete: ReferentialAction,
    /// The action to perform when the referenced row is updated.
    pub on_update: ReferentialAction,
}

#[derive(PartialEq, Debug, Clone, Eq, Hash, Serialize, Deserialize, Display)]
pub enum ReferentialAction {
    #[strum(to_string = "NO ACTION")]
    /// A NO ACTION constraint specifies that when a referenced row is deleted,
    /// no action should be taken.
    NoAction,
    #[strum(to_string = "CASCADE")]
    /// A CASCADE constraint specifies that when a referenced row is deleted,
    /// row(s) that reference it should also be deleted.
    Cascade,
    #[strum(to_string = "SET NULL")]
    /// A SET NULL constraint specifies that when a referenced row is deleted,
    /// row(s) that reference it should have their referencing column(s) set to NULL.
    SetNull,
    #[strum(to_string = "SET DEFAULT")]
    /// A SET DEFAULT constraint specifies that when a referenced row is deleted,
    /// row(s) that reference it should have their referencing column(s) set to the column's default value.
    SetDefault,
}

impl From<sqlparser::ast::ReferentialAction> for ReferentialAction {
    fn from(action: sqlparser::ast::ReferentialAction) -> Self {
        match action {
            sqlparser::ast::ReferentialAction::Restrict
            | sqlparser::ast::ReferentialAction::NoAction => ReferentialAction::NoAction,
            sqlparser::ast::ReferentialAction::Cascade => ReferentialAction::Cascade,
            sqlparser::ast::ReferentialAction::SetNull => ReferentialAction::SetNull,
            sqlparser::ast::ReferentialAction::SetDefault => ReferentialAction::SetDefault,
        }
    }
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
        columns: Option<Vec<ColumnDef>>,
        source: Option<Box<Query>>,
        engine: Option<String>,
        foreign_keys: Vec<ForeignKey>,
        primary_key: Option<Vec<usize>>,
        unique_constraints: Vec<UniqueConstraint>,
        comment: Option<String>,
    },
    /// CREATE FUNCTION
    CreateFunction {
        or_replace: bool,
        name: String,
        /// Optional schema
        args: Vec<OperateFunctionArg>,
        return_: Expr,
    },
    /// ALTER TABLE
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
        /// An optional `CASCADE` clause for dropping dependent constructs.
        cascade: bool,
    },
    /// DROP FUNCTION
    DropFunction {
        /// An optional `IF EXISTS` clause. (Non-standard.)
        if_exists: bool,
        /// One or more objects to drop. (ANSI SQL requires exactly one.)
        names: Vec<String>,
    },
    /// CREATE INDEX
    CreateIndex {
        name: String,
        table_name: String,
        column: OrderByExpr,
    },
    /// DROP INDEX
    DropIndex {
        name: String,
        table_name: String,
    },
    /// START TRANSACTION, BEGIN
    StartTransaction,
    /// COMMIT
    Commit,
    /// ROLLBACK
    Rollback,
    /// SHOW VARIABLE
    ShowVariable(Variable),
    ShowIndexes(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Assignment {
    pub id: String,
    pub value: Expr,
}

impl Assignment {
    pub fn new(id: String, value: Expr) -> Self {
        Self { id, value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Variable {
    Tables,
    Functions,
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
                            r#"UPDATE "{table_name}" SET {assignments} WHERE {};"#,
                            expr.to_sql()
                        )
                    }
                    None => format!(r#"UPDATE "{table_name}" SET {assignments};"#),
                }
            }
            Statement::Delete {
                table_name,
                selection,
            } => match selection {
                Some(expr) => format!(r#"DELETE FROM "{table_name}" WHERE {};"#, expr.to_sql()),
                None => format!(r#"DELETE FROM "{table_name}";"#),
            },
            Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                source,
                engine,
                foreign_keys,
                primary_key,
                unique_constraints,
                comment,
            } => {
                let if_not_exists = if_not_exists.then_some("IF NOT EXISTS");

                let primary_key = match (primary_key.as_ref(), columns.as_ref()) {
                    (Some(indices), Some(columns)) => Some(format!(
                        "PRIMARY KEY ({})",
                        indices
                            .iter()
                            .copied()
                            .map(|i| format!(r#""{}""#, columns[i].name))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )),
                    _ => None,
                };

                let body = match (source, columns) {
                    (Some(query), _) => Some(format!("AS {}", query.to_sql())),
                    (None, None) => None,
                    (None, Some(columns)) => {
                        let column_names = columns
                            .iter()
                            .map(|column| &column.name)
                            .collect::<Vec<_>>();
                        let body =
                            columns
                                .iter()
                                .map(ToSql::to_sql)
                                .chain(foreign_keys.iter().map(ToSql::to_sql))
                                .chain(unique_constraints.iter().map(|unique_constraint| {
                                    unique_constraint.to_sql(&column_names)
                                }))
                                .chain(primary_key)
                                .collect::<Vec<_>>()
                                .join(", ");

                        Some(format!("({body})"))
                    }
                };
                let engine = engine.as_ref().map(|engine| format!("ENGINE = {engine}"));
                let comment = comment
                    .as_ref()
                    .map(|comment| format!("COMMENT = '{comment}'"));
                let sql = vec![
                    Some("CREATE TABLE"),
                    if_not_exists,
                    Some(&format! {r#""{name}""#}),
                    body.as_deref(),
                    engine.as_deref(),
                    comment.as_deref(),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<&str>>()
                .join(" ");

                format!("{sql};")
            }
            Statement::CreateFunction {
                or_replace,
                name,
                args,
                return_,
                ..
            } => {
                let or_replace = or_replace.then_some(" OR REPLACE").unwrap_or("");
                let args = args
                    .iter()
                    .map(ToSql::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ");
                let return_ = format!(" RETURN {}", return_.to_sql());
                format!("CREATE{or_replace} FUNCTION {name}({args}){return_};")
            }
            Statement::AlterTable { name, operation } => {
                format!(r#"ALTER TABLE "{name}" {};"#, operation.to_sql())
            }
            Statement::DropTable {
                if_exists,
                names,
                cascade,
            } => {
                let if_exists = if_exists.then_some("IF EXISTS").unwrap_or_default();
                let names = names
                    .iter()
                    .map(|name| format!(r#""{name}""#))
                    .collect::<Vec<_>>()
                    .join(", ");
                let cascade = cascade.then_some("CASCADE").unwrap_or_default();

                vec!["DROP TABLE", if_exists, &names, cascade]
                    .into_iter()
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
                    .join(" ")
                    + ";"
            }
            Statement::DropFunction { if_exists, names } => {
                let names = names.join(", ");
                match if_exists {
                    true => format!("DROP FUNCTION IF EXISTS {};", names),
                    false => format!("DROP FUNCTION {};", names),
                }
            }
            Statement::CreateIndex {
                name,
                table_name,
                column,
            } => {
                format!(
                    r#"CREATE INDEX "{name}" ON "{table_name}" ({});"#,
                    column.to_sql()
                )
            }
            Statement::DropIndex { name, table_name } => {
                format!("DROP INDEX {table_name}.{name};")
            }
            Statement::StartTransaction => "START TRANSACTION;".to_owned(),
            Statement::Commit => "COMMIT;".to_owned(),
            Statement::Rollback => "ROLLBACK;".to_owned(),
            Statement::ShowVariable(variable) => match variable {
                Variable::Tables => "SHOW TABLES;".to_owned(),
                Variable::Functions => "SHOW FUNCTIONS;".to_owned(),
                Variable::Version => "SHOW VERSIONS;".to_owned(),
            },
            Statement::ShowIndexes(object_name) => {
                format!(r#"SHOW INDEXES FROM "{object_name}";"#)
            }
            _ => "(..statement..)".to_owned(),
        }
    }
}

impl ToSql for Assignment {
    fn to_sql(&self) -> String {
        format!(r#""{}" = {}"#, self.id, self.value.to_sql())
    }
}

impl ToSql for ForeignKey {
    fn to_sql(&self) -> String {
        let ForeignKey {
            referencing_column_name,
            referenced_table_name,
            referenced_column_name,
            name,
            on_delete,
            on_update,
        } = self;

        format!(
            r#"CONSTRAINT "{}" FOREIGN KEY ("{}") REFERENCES "{}" ("{}") ON DELETE {} ON UPDATE {}"#,
            name,
            referencing_column_name,
            referenced_table_name,
            referenced_column_name,
            on_delete,
            on_update
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Array {
    pub elem: Vec<Expr>,
    pub named: bool,
}

#[cfg(test)]
mod tests {
    use {
        crate::ast::{
            AlterTableOperation, Assignment, AstLiteral, BinaryOperator, ColumnDef, DataType, Expr,
            ForeignKey, OperateFunctionArg, OrderByExpr, Query, ReferentialAction, Select,
            SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, ToSql, UniqueConstraint,
            Values, Variable,
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
            r#"UPDATE "Foo" SET "id" = 4, "color" = 'blue';"#,
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
            r#"UPDATE "Foo" SET "name" = 'first' WHERE "a" > "b";"#,
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
            r#"DELETE FROM "Foo";"#,
            Statement::Delete {
                table_name: "Foo".into(),
                selection: None
            }
            .to_sql()
        );

        assert_eq!(
            r#"DELETE FROM "Foo" WHERE "item" = 'glue';"#,
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
            r#"CREATE TABLE IF NOT EXISTS "Foo";"#,
            Statement::CreateTable {
                if_not_exists: true,
                name: "Foo".into(),
                columns: None,
                source: None,
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: Vec::new(),
                comment: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#"CREATE TABLE "Foo";"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: None,
                source: None,
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: Vec::new(),
                comment: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#"CREATE TABLE IF NOT EXISTS "Foo" ("id" BOOLEAN NOT NULL) COMMENT = 'this is comment';"#,
            Statement::CreateTable {
                if_not_exists: true,
                name: "Foo".into(),
                columns: Some(vec![ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    default: None,
                    comment: None,
                },]),
                source: None,
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: Vec::new(),
                comment: Some("this is comment".to_owned()),
            }
            .to_sql()
        );

        assert_eq!(
            r#"CREATE TABLE "Foo" ("id" INT NOT NULL, "num" INT NULL, "name" TEXT NOT NULL);"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: Some(vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        comment: None,
                    },
                    ColumnDef {
                        name: "num".to_owned(),
                        data_type: DataType::Int,
                        nullable: true,
                        default: None,
                        comment: None,
                    },
                    ColumnDef {
                        name: "name".to_owned(),
                        data_type: DataType::Text,
                        nullable: false,
                        default: None,
                        comment: None,
                    }
                ]),
                source: None,
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: Vec::new(),
                comment: None,
            }
            .to_sql()
        );
    }

    #[test]
    /// Test to evaluate whether the `CREATE TABLE` statement involving UNIQUE constraints can be converted to SQL.
    fn to_sql_create_table_with_unique() {
        assert_eq!(
            r#"CREATE TABLE "Foo" ("id" INT NOT NULL, "name" TEXT NOT NULL, UNIQUE ("id"));"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: Some(vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        comment: None,
                    },
                    ColumnDef {
                        name: "name".to_owned(),
                        data_type: DataType::Text,
                        nullable: false,
                        default: None,
                        comment: None,
                    }
                ]),
                source: None,
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: vec![UniqueConstraint::new(None, vec![0])],
                comment: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#"CREATE TABLE "Foo" ("id" INT NOT NULL, "name" TEXT NOT NULL, UNIQUE ("id"));"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: Some(vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        comment: None,
                    },
                    ColumnDef {
                        name: "name".to_owned(),
                        data_type: DataType::Text,
                        nullable: false,
                        default: None,
                        comment: None,
                    }
                ]),
                source: None,
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: vec![UniqueConstraint::new(None, vec![0])],
                comment: None,
            }
            .to_sql()
        );

        // Next, we try one with a named constraint.
        assert_eq!(
            r#"CREATE TABLE "Foo" ("id" INT NOT NULL, "name" TEXT NOT NULL, CONSTRAINT "unique_id" UNIQUE ("id"));"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: Some(vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        comment: None,
                    },
                    ColumnDef {
                        name: "name".to_owned(),
                        data_type: DataType::Text,
                        nullable: false,
                        default: None,
                        comment: None,
                    }
                ]),
                source: None,
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: vec![UniqueConstraint::new(
                    Some("unique_id".to_owned()),
                    vec![0]
                )],
                comment: None,
            }
            .to_sql()
        );

        // And one involving multiple columns, with no name.
        assert_eq!(
            r#"CREATE TABLE "Foo" ("id" INT NOT NULL, "name" TEXT NOT NULL, UNIQUE ("id", "name"));"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: Some(vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        comment: None,
                    },
                    ColumnDef {
                        name: "name".to_owned(),
                        data_type: DataType::Text,
                        nullable: false,
                        default: None,
                        comment: None,
                    }
                ]),
                source: None,
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: vec![UniqueConstraint::new(None, vec![0, 1])],
                comment: None,
            }
            .to_sql()
        );

        // And multiple constraint at ones.
        assert_eq!(
            r#"CREATE TABLE "Foo" ("id" INT NOT NULL, "name" TEXT NOT NULL, UNIQUE ("id"), UNIQUE ("name"), CONSTRAINT "my_unique" UNIQUE ("id", "name"));"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: Some(vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        comment: None,
                    },
                    ColumnDef {
                        name: "name".to_owned(),
                        data_type: DataType::Text,
                        nullable: false,
                        default: None,
                        comment: None,
                    }
                ]),
                source: None,
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: vec![
                    UniqueConstraint::new(None, vec![0]),
                    UniqueConstraint::new(None, vec![1]),
                    UniqueConstraint::new(Some("my_unique".to_owned()), vec![0, 1]),
                ],
                comment: None,
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_create_table_as() {
        assert_eq!(
            r#"CREATE TABLE "Foo" AS SELECT "id", "count" FROM "Bar";"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: None,
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
                })),
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: Vec::new(),
                comment: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#"CREATE TABLE IF NOT EXISTS "Foo" AS VALUES (TRUE);"#,
            Statement::CreateTable {
                if_not_exists: true,
                name: "Foo".into(),
                columns: None,
                source: Some(Box::new(Query {
                    body: SetExpr::Values(Values(vec![vec![Expr::Literal(AstLiteral::Boolean(
                        true
                    ))]])),
                    order_by: vec![],
                    limit: None,
                    offset: None
                })),
                engine: None,
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: Vec::new(),
                comment: None,
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_create_table_with_engine() {
        assert_eq!(
            r#"CREATE TABLE "Foo" ENGINE = MEMORY;"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: None,
                source: None,
                engine: Some("MEMORY".to_owned()),
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: Vec::new(),
                comment: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#"CREATE TABLE "Foo" ("id" BOOLEAN NOT NULL) ENGINE = SLED;"#,
            Statement::CreateTable {
                if_not_exists: false,
                name: "Foo".into(),
                columns: Some(vec![ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    default: None,
                    comment: None,
                },]),
                source: None,
                engine: Some("SLED".to_owned()),
                foreign_keys: Vec::new(),
                primary_key: None,
                unique_constraints: Vec::new(),
                comment: None,
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_insert_function() {
        assert_eq!(
            r#"CREATE FUNCTION add("num" INT DEFAULT 0) RETURN "num";"#,
            Statement::CreateFunction {
                or_replace: false,
                name: "add".into(),
                args: vec![OperateFunctionArg {
                    name: "num".into(),
                    data_type: DataType::Int,
                    default: Some(Expr::Literal(AstLiteral::Number(
                        BigDecimal::from_str("0").unwrap()
                    ))),
                }],
                return_: Expr::Identifier("num".to_owned())
            }
            .to_sql()
        );
        assert_eq!(
            "CREATE OR REPLACE FUNCTION add() RETURN 1;",
            Statement::CreateFunction {
                or_replace: true,
                name: "add".into(),
                args: vec![],
                return_: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1").unwrap()))
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_alter_table() {
        assert_eq!(
            r#"ALTER TABLE "Foo" ADD COLUMN "amount" INT NOT NULL DEFAULT 10;"#,
            Statement::AlterTable {
                name: "Foo".into(),
                operation: AlterTableOperation::AddColumn {
                    column_def: ColumnDef {
                        name: "amount".to_owned(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: Some(Expr::Literal(AstLiteral::Number(
                            BigDecimal::from_str("10").unwrap()
                        ))),
                        comment: None,
                    },
                    unique: false
                }
            }
            .to_sql()
        );

        assert_eq!(
            r#"ALTER TABLE "Foo" ADD UNIQUE COLUMN "amount" INT NOT NULL DEFAULT 10;"#,
            Statement::AlterTable {
                name: "Foo".into(),
                operation: AlterTableOperation::AddColumn {
                    column_def: ColumnDef {
                        name: "amount".to_owned(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: Some(Expr::Literal(AstLiteral::Number(
                            BigDecimal::from_str("10").unwrap()
                        ))),
                        comment: None,
                    },
                    unique: true
                }
            }
            .to_sql()
        );

        assert_eq!(
            r#"ALTER TABLE "Foo" DROP COLUMN "something";"#,
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
            r#"ALTER TABLE "Foo" DROP COLUMN IF EXISTS "something";"#,
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
            r#"ALTER TABLE "Bar" RENAME COLUMN "id" TO "new_id";"#,
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
            r#"ALTER TABLE "Foo" RENAME TO "Bar";"#,
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
            r#"DROP TABLE "Test";"#,
            Statement::DropTable {
                if_exists: false,
                names: vec!["Test".into()],
                cascade: false,
            }
            .to_sql()
        );

        assert_eq!(
            r#"DROP TABLE IF EXISTS "Test";"#,
            Statement::DropTable {
                if_exists: true,
                names: vec!["Test".into()],
                cascade: false,
            }
            .to_sql()
        );

        assert_eq!(
            r#"DROP TABLE "Foo", "Bar";"#,
            Statement::DropTable {
                if_exists: false,
                names: vec!["Foo".into(), "Bar".into(),],
                cascade: false,
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_delete_function() {
        assert_eq!(
            "DROP FUNCTION Test;",
            Statement::DropFunction {
                if_exists: false,
                names: vec!["Test".into()]
            }
            .to_sql()
        );

        assert_eq!(
            "DROP FUNCTION IF EXISTS Test;",
            Statement::DropFunction {
                if_exists: true,
                names: vec!["Test".into()]
            }
            .to_sql()
        );

        assert_eq!(
            "DROP FUNCTION Foo, Bar;",
            Statement::DropFunction {
                if_exists: false,
                names: vec!["Foo".into(), "Bar".into(),]
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_create_index() {
        assert_eq!(
            r#"CREATE INDEX "idx_name" ON "Test" ("LastName");"#,
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
            "SHOW FUNCTIONS;",
            Statement::ShowVariable(Variable::Functions).to_sql()
        );
        assert_eq!(
            "SHOW VERSIONS;",
            Statement::ShowVariable(Variable::Version).to_sql()
        );
    }

    #[test]
    fn to_sql_show_indexes() {
        assert_eq!(
            r#"SHOW INDEXES FROM "Test";"#,
            Statement::ShowIndexes("Test".into()).to_sql()
        );
    }

    #[test]
    fn to_sql_assignment() {
        assert_eq!(
            r#""count" = 5"#,
            Assignment {
                id: "count".to_owned(),
                value: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("5").unwrap()))
            }
            .to_sql()
        )
    }

    #[test]
    fn to_sql_foreign_key() {
        assert_eq!(
            r#"CONSTRAINT "fk_id" FOREIGN KEY ("id") REFERENCES "Test" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION"#,
            ForeignKey {
                name: "fk_id".into(),
                referencing_column_name: "id".into(),
                referenced_table_name: "Test".into(),
                referenced_column_name: "id".into(),
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
            }
            .to_sql()
        )
    }
}
