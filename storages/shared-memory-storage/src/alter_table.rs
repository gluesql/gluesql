use {
    super::{SharedMemoryStorage, lock_error},
    gluesql_core::{ast::ColumnDef, error::Result, store::AlterTable},
};

impl AlterTable for SharedMemoryStorage {
    fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        let mut database = self.database.write().map_err(lock_error)?;

        database.rename_schema(table_name, new_table_name)
    }

    fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        let mut database = self.database.write().map_err(lock_error)?;

        database.rename_column(table_name, old_column_name, new_column_name)
    }

    fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()> {
        let mut database = self.database.write().map_err(lock_error)?;

        database.add_column(table_name, column_def)
    }

    fn drop_column(&mut self, table_name: &str, column_name: &str, if_exists: bool) -> Result<()> {
        let mut database = self.database.write().map_err(lock_error)?;

        database.drop_column(table_name, column_name, if_exists)
    }
}
