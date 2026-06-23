use {
    crate::MemoryStorage,
    gluesql_core::{
        error::Result,
        store::{MetaIter, Metadata},
    },
};

impl Metadata for MemoryStorage {
    fn scan_table_meta(&self) -> Result<MetaIter> {
        let meta = self.metadata.clone().into_iter().map(Ok);

        Ok(Box::new(meta))
    }
}
