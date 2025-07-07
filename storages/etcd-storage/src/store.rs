use {
    crate::EtcdStorage,
    async_trait::async_trait,
    futures::stream,
    gluesql_core::{
        data::{Key, Schema},
        error::{Error, Result},
        store::{DataRow, RowIter, Store},
    },
};

#[async_trait(?Send)]
impl Store for EtcdStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let prefix = format!("{}/schema/", self.prefix);
        let resp = self
            .client
            .kv_client()
            .get(
                prefix.clone(),
                Some(etcd_client::GetOptions::new().with_prefix()),
            )
            .await
            .map_err(|e| Error::StorageMsg(e.to_string()))?;
        let mut schemas = Vec::new();
        for kv in resp.kvs() {
            let value = kv
                .value_str()
                .map_err(|e| Error::StorageMsg(e.to_string()))?;
            if let Ok(schema) = Schema::from_ddl(value) {
                schemas.push(schema);
            }
        }
        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));
        Ok(schemas)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let key = self.schema_key(table_name);
        let resp = self
            .client
            .kv_client()
            .get(key, None)
            .await
            .map_err(|e| Error::StorageMsg(e.to_string()))?;
        if let Some(kv) = resp.kvs().first() {
            let value = kv
                .value_str()
                .map_err(|e| Error::StorageMsg(e.to_string()))?;
            Ok(Some(Schema::from_ddl(value)?))
        } else {
            Ok(None)
        }
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        let key = self.data_key(table_name, key)?;
        let resp = self
            .client
            .kv_client()
            .get(key, None)
            .await
            .map_err(|e| Error::StorageMsg(e.to_string()))?;
        if let Some(kv) = resp.kvs().first() {
            let value = kv
                .value_str()
                .map_err(|e| Error::StorageMsg(e.to_string()))?;
            let row = serde_json::from_str(value).map_err(|e| Error::StorageMsg(e.to_string()))?;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let prefix = self.data_prefix(table_name);
        let resp = self
            .client
            .kv_client()
            .get(
                prefix.clone(),
                Some(etcd_client::GetOptions::new().with_prefix()),
            )
            .await
            .map_err(|e| Error::StorageMsg(e.to_string()))?;
        let rows = resp
            .kvs()
            .iter()
            .map(|kv| {
                let key_str = kv.key_str().map_err(|e| Error::StorageMsg(e.to_string()))?;
                let key_part = key_str
                    .strip_prefix(&prefix)
                    .ok_or_else(|| Error::StorageMsg("invalid key".to_owned()))?;
                let key: Key =
                    serde_json::from_str(key_part).map_err(|e| Error::StorageMsg(e.to_string()))?;
                let value = kv
                    .value_str()
                    .map_err(|e| Error::StorageMsg(e.to_string()))?;
                let row: DataRow =
                    serde_json::from_str(value).map_err(|e| Error::StorageMsg(e.to_string()))?;
                Ok((key, row))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::pin(stream::iter(rows.into_iter().map(Ok))))
    }
}
