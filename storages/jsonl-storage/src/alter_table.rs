#![cfg(feature = "alter-table")]

use {
    super::JsonlStorage,
    async_trait::async_trait,
    gluesql_core::{ast::ColumnDef, result::Result, store::AlterTable},
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
    async fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        JsonlStorage::rename_schema(self, table_name, new_table_name)
    }

    async fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        JsonlStorage::rename_column(self, table_name, old_column_name, new_column_name)
    }

    async fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()> {
        JsonlStorage::add_column(self, table_name, column_def)
    }

    async fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        JsonlStorage::drop_column(self, table_name, column_name, if_exists)
    }
}
