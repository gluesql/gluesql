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
        let items = database.items.read().await;

        let mut names: Vec<_> = items.keys().map(Clone::clone).collect();
        names.sort();

        Ok(names)
    }
}
