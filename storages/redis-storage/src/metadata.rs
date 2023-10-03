use {
    crate::RedisStorage,
    async_trait::async_trait,
    gluesql_core::{
        error::Result,
        store::{MetaIter, Metadata},
    },
};

#[async_trait(?Send)]
impl Metadata for RedisStorage {
    async fn scan_table_meta(&self) -> Result<MetaIter> {
        let meta = self.metadata.clone().into_iter().map(Ok);

        Ok(Box::new(meta))
    }
}
