use async_trait::async_trait;
use sled::IVec;

use super::{err_into, fetch_schema, SledStorage};
#[cfg(feature = "auto-increment")]
use crate::data::Value;
use crate::{Result, RowIter, Schema, Store};
use fstrings::*;

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

    #[cfg(feature = "auto-increment")]
    async fn get_generator(&self, table_name: &str, column_name: &str) -> Result<Value> {
        let value = self
            .tree
            .get(f!("generator/{table_name}/{column_name}").as_bytes())
            .map_err(err_into)?
            .ok_or_else(|| err_into(sled::Error::Unsupported("Generator unset".to_string())))?;
        let value = bincode::deserialize(&value).map_err(err_into)?;
        Ok(value)
    }
}
