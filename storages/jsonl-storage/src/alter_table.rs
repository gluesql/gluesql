#![cfg(feature = "alter-table")]

use {
    super::JsonlStorage,
    async_trait::async_trait,
    gluesql_core::{
        ast::ColumnDef,
        result::{MutResult, Result, TrySelf},
        store::AlterTable,
    },
};

impl JsonlStorage {
    pub fn rename_schema(&mut self, _table_name: &str, _new_table_name: &str) -> Result<()> {
        todo!();
    }

    pub fn rename_column(
        &mut self,
        _table_name: &str,
        _old_column_name: &str,
        _new_column_name: &str,
    ) -> Result<()> {
        todo!();
    }

    pub fn add_column(&mut self, _table_name: &str, _column_def: &ColumnDef) -> Result<()> {
        todo!();
    }

    pub fn drop_column(
        &mut self,
        _table_name: &str,
        _column_name: &str,
        _if_exists: bool,
    ) -> Result<()> {
        todo!();
    }
}

#[async_trait(?Send)]
impl AlterTable for JsonlStorage {
    async fn rename_schema(self, table_name: &str, new_table_name: &str) -> MutResult<Self, ()> {
        let mut storage = self;

        JsonlStorage::rename_schema(&mut storage, table_name, new_table_name).try_self(storage)
    }

    async fn rename_column(
        self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> MutResult<Self, ()> {
        let mut storage = self;

        JsonlStorage::rename_column(&mut storage, table_name, old_column_name, new_column_name)
            .try_self(storage)
    }

    async fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()> {
        let mut storage = self;

        JsonlStorage::add_column(&mut storage, table_name, column_def).try_self(storage)
    }

    async fn drop_column(
        self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> MutResult<Self, ()> {
        let mut storage = self;

        JsonlStorage::drop_column(&mut storage, table_name, column_name, if_exists)
            .try_self(storage)
    }
}
