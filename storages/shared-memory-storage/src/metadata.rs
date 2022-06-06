use {
    super::SharedMemoryStorage,
    async_trait::async_trait,
    gluesql_core::{result::Result, store::Metadata},
    std::sync::Arc,
};

#[async_trait(?Send)]
impl Metadata for SharedMemoryStorage {
    async fn schema_names(&self) -> Result<Vec<String>> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;

        database.schema_names().await
    }
}
