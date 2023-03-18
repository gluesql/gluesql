use {
    crate::MemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        prelude::Value,
        result::{Error, Result},
        store::{MetaIter, Metadata},
    },
};

#[async_trait(?Send)]
impl Metadata for MemoryStorage {
    async fn scan_meta(&self) -> Result<MetaIter> {
        let meta = self
            .metadata
            .clone()
            .into_iter()
            .map(|(name, value)| match value {
                Value::Map(map) => Ok((name, map)),
                _ => Err(Error::StorageMsg("Invalid metadata".to_owned())),
            });

        Ok(Box::new(meta))
    }
}
