use {
    super::SharedMemoryStorage,
    async_trait::async_trait,
    gluesql_core::{ast::ColumnDef, result::MutResult, store::AlterTable},
    memory_storage::MemoryStorage,
    std::sync::Arc,
};

#[async_trait(?Send)]
impl AlterTable for SharedMemoryStorage {
    async fn rename_schema(self, table_name: &str, new_table_name: &str) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        if let Err(error) = MemoryStorage::rename_schema(&mut database, table_name, new_table_name)
        {
            return Err((self, error));
        }

        Ok((self, ()))
    }

    async fn rename_column(
        self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        if let Err(error) = MemoryStorage::rename_column(
            &mut database,
            table_name,
            old_column_name,
            new_column_name,
        ) {
            return Err((self, error));
        }

        Ok((self, ()))
    }

    async fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        if let Err(error) = MemoryStorage::add_column(&mut database, table_name, column_def) {
            return Err((self, error));
        }

        Ok((self, ()))
    }

    async fn drop_column(
        self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> MutResult<Self, ()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;

        if let Err(error) =
            MemoryStorage::drop_column(&mut database, table_name, column_name, if_exists)
        {
            return Err((self, error));
        }

        Ok((self, ()))
    }
}
