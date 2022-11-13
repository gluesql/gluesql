use {
    super::IndexeddbStorage,
    async_trait::async_trait,
    gluesql_core::{
        ast::ColumnDef,
        data::Value,
        result::{Error, MutResult, Result, TrySelf},
        store::AlterTable,
        store::AlterTableError,
    },
};

#[async_trait(?Send)]
impl AlterTable for IndexeddbStorage {
    async fn rename_schema(self, table_name: &str, new_table_name: &str) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[IndexeddbStorage] AlterTable is not supported".to_owned()),
        ))
    }

    async fn rename_column(
        self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[IndexeddbStorage] AlterTable is not supported".to_owned()),
        ))
    }

    async fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[IndexeddbStorage] AlterTable is not supported".to_owned()),
        ))
    }

    async fn drop_column(
        self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[IndexeddbStorage] AlterTable is not supported".to_owned()),
        ))
    }
}
