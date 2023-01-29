use {
    crate::{
        ast::ColumnDef,
        result::{Error, MutResult},
    },
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

#[async_trait(?Send)]
pub trait AlterTable
where
    Self: Sized,
{
    async fn rename_schema(self, _table_name: &str, _new_table_name: &str) -> MutResult<Self, ()> {
        let msg = "[Storage] AlterTable::rename_schema is not supported".to_owned();

        Err((self, Error::StorageMsg(msg)))
    }

    async fn rename_column(
        self,
        _table_name: &str,
        _old_column_name: &str,
        _new_column_name: &str,
    ) -> MutResult<Self, ()> {
        let msg = "[Storage] AlterTable::rename_column is not supported".to_owned();

        Err((self, Error::StorageMsg(msg)))
    }

    async fn add_column(self, _table_name: &str, _column_def: &ColumnDef) -> MutResult<Self, ()> {
        let msg = "[Storage] AlterTable::add_column is not supported".to_owned();

        Err((self, Error::StorageMsg(msg)))
    }

    async fn drop_column(
        self,
        _table_name: &str,
        _column_name: &str,
        _if_exists: bool,
    ) -> MutResult<Self, ()> {
        let msg = "[Storage] AlterTable::drop_column is not supported".to_owned();

        Err((self, Error::StorageMsg(msg)))
    }
}
