use async_trait::async_trait;
use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::ColumnDef;

use crate::result::MutResult;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum AlterTableError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Renaming column not found")]
    RenamingColumnNotFound,

    #[error("Default value is required: {0}")]
    DefaultValueRequired(String),

    #[error("Adding column already exists: {0}")]
    AddingColumnAlreadyExists(String),

    #[error("Dropping column not found: {0}")]
    DroppingColumnNotFound(String),
}

#[async_trait]
pub trait AlterTable
where
    Self: Sized,
{
    async fn rename_schema(self, table_name: &str, new_table_name: &str) -> MutResult<Self, ()>;

    async fn rename_column(
        self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> MutResult<Self, ()>;

    async fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()>;

    async fn drop_column(
        self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> MutResult<Self, ()>;
}
