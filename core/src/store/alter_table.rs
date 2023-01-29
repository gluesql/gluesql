#![cfg(feature = "alter-table")]

use {
    crate::{ast::ColumnDef, result::Result},
    async_trait::async_trait,
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum AlterTableError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Renaming column not found")]
    RenamingColumnNotFound,

    #[error("Default value is required: {0:#?}")]
    DefaultValueRequired(ColumnDef),

    #[error("Already existing column: {0}")]
    AlreadyExistingColumn(String),

    #[error("Dropping column not found: {0}")]
    DroppingColumnNotFound(String),

    #[error("Schemaless table does not support ALTER TABLE: {0}")]
    SchemalessTableFound(String),
}

#[async_trait]
pub trait AlterTable {
    async fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()>;

    async fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()>;

    async fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()>;

    async fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()>;
}
