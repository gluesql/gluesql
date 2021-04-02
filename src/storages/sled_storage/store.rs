use {
    super::{err_into, fetch_schema, SledStorage},
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
        let prefix = format!("data/{}/", table_name);

        let result_set = self.tree.scan_prefix(prefix.as_bytes()).map(move |item| {
            let (key, value) = item.map_err(err_into)?;
            let value = bincode::deserialize(&value).map_err(err_into)?;

            Ok((key, value))
        });

        Ok(Box::new(result_set))
    }
}
