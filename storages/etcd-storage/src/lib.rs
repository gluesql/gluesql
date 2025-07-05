#![deny(clippy::str_to_string)]

mod store;
mod store_mut;

use gluesql_core::{
    data::Key,
    error::Result,
    store::{
        AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, Transaction,
    },
};

pub struct EtcdStorage {
    pub client: etcd_client::Client,
    pub prefix: String,
}

impl EtcdStorage {
    pub async fn new(endpoints: &[&str], prefix: &str) -> Result<Self> {
        let client = etcd_client::Client::connect(endpoints, None)
            .await
            .map_err(|e| gluesql_core::error::Error::StorageMsg(e.to_string()))?;
        Ok(Self {
            client,
            prefix: prefix.to_owned(),
        })
    }

    fn schema_key(&self, table_name: &str) -> String {
        format!("{}/schema/{}", self.prefix, table_name)
    }

    fn data_key(&self, table_name: &str, key: &Key) -> Result<String> {
        let key_ser = serde_json::to_string(key)
            .map_err(|e| gluesql_core::error::Error::StorageMsg(e.to_string()))?;
        Ok(format!("{}/data/{}/{}", self.prefix, table_name, key_ser))
    }

    fn data_prefix(&self, table_name: &str) -> String {
        format!("{}/data/{}/", self.prefix, table_name)
    }
}

impl AlterTable for EtcdStorage {}
impl Index for EtcdStorage {}
impl IndexMut for EtcdStorage {}
impl Transaction for EtcdStorage {}
impl Metadata for EtcdStorage {}
impl CustomFunction for EtcdStorage {}
impl CustomFunctionMut for EtcdStorage {}
