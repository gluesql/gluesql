use {
    super::MemoryStorage,
    async_trait::async_trait,
    gluesql_core::{result::Result, store::Metadata},
};

#[async_trait(?Send)]
impl Metadata for MemoryStorage {
    async fn schema_names(&self) -> Result<Vec<String>> {
        let mut names: Vec<_> = self.items.keys().map(Clone::clone).collect();
        names.sort();

        Ok(names)
    }
}
