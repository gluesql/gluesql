use {
    super::{fetch_schema, scan_data, SledStorage},
    crate::{Result, RowIter, Schema, Store},
    async_trait::async_trait,
    sled::IVec,
};

#[cfg(feature = "index")]
use {
    super::err_into,
    crate::{IndexError, Value},
};

#[async_trait(?Send)]
impl Store<IVec> for SledStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        fetch_schema(&self.tree, table_name).map(|(_, schema)| schema)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<IVec>> {
        Ok(scan_data(&self.tree, table_name))
    }

    #[cfg(feature = "index")]
    async fn scan_indexed_data(
        &self,
        table_name: &str,
        index_name: &str,
        value: Value,
    ) -> Result<RowIter<IVec>> {
        let index_key = format!(
            "index/{}/{}/{}",
            table_name,
            index_name,
            String::from(value)
        );
        let index_data_id = self
            .tree
            .get(&index_key.as_bytes())
            .map_err(err_into)?
            .ok_or(IndexError::ConflictOnEmptyIndexDataIdScan)?;

        let bytes = "indexdata/"
            .to_owned()
            .into_bytes()
            .into_iter()
            .chain(index_data_id.iter().copied())
            .collect::<Vec<_>>();
        let index_data_prefix = IVec::from(bytes);

        let tree = self.tree.clone();
        let rows = self.tree.scan_prefix(&index_data_prefix).map(move |item| {
            let (_, data_key) = item.map_err(err_into)?;
            let value = tree
                .get(&data_key)
                .map_err(err_into)?
                .ok_or(IndexError::ConflictOnEmptyIndexValueScan)?;
            let value = bincode::deserialize(&value).map_err(err_into)?;

            Ok((data_key, value))
        });

        Ok(Box::new(rows))
    }
}
