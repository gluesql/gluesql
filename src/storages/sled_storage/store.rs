use {
    super::{fetch_schema, scan_data, SledStorage},
    crate::{Result, RowIter, Schema, Store},
    async_trait::async_trait,
    sled::IVec,
};

#[async_trait(?Send)]
impl Store<IVec> for SledStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        fetch_schema(&self.tree, table_name).map(|(_, schema)| schema)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<IVec>> {
        Ok(scan_data(&self.tree, table_name))
    }
}
