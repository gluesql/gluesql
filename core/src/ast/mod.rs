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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Statement {
    ShowColumns {
        table_name: ObjectName,
    },
    /// SELECT
    Query(Box<Query>),
    /// INSERT
    Insert {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<String>,
        /// A SQL query that specifies what to insert
        source: Box<Query>,
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
