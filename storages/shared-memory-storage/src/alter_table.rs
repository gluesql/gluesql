use {
    super::SharedMemoryStorage,
    async_trait::async_trait,
    gluesql_core::{ast::ColumnDef, error::Result, store::AlterTable},
    std::sync::Arc,
};

#[async_trait(?Send)]
impl AlterTable for SharedMemoryStorage {
    async fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        database.rename_schema(table_name, new_table_name).await
    }

    async fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        database
            .rename_column(table_name, old_column_name, new_column_name)
            .await
    }

    async fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        database.add_column(table_name, column_def).await
    }

    async fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        database
            .drop_column(table_name, column_name, if_exists)
            .await
    }
}
