use {
    crate::{ast::ColumnDef, result::MutResult},
    async_trait::async_trait,
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

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

#[async_trait(?Send)]
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
