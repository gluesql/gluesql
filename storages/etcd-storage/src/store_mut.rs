use {
    crate::EtcdStorage,
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::{Error, Result},
        store::{DataRow, StoreMut},
    },
};

#[async_trait(?Send)]
impl StoreMut for EtcdStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let key = self.schema_key(&schema.table_name);
        let ddl = schema.to_ddl();
        self.client
            .kv_client()
            .put(key, ddl, None)
            .await
            .map_err(|e| Error::StorageMsg(e.to_string()))?;
        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let schema_key = self.schema_key(table_name);
        self.client
            .kv_client()
            .delete(schema_key, None)
            .await
            .map_err(|e| Error::StorageMsg(e.to_string()))?;
        let data_prefix = self.data_prefix(table_name);
        self.client
            .kv_client()
            .delete(
                data_prefix,
                Some(etcd_client::DeleteOptions::new().with_prefix()),
            )
            .await
            .map_err(|e| Error::StorageMsg(e.to_string()))?;
        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        for row in rows {
            let key = Key::Uuid(uuid::Uuid::now_v7().as_u128());
            let data_key = self.data_key(table_name, &key)?;
            let value =
                serde_json::to_string(&row).map_err(|e| Error::StorageMsg(e.to_string()))?;
            self.client
                .kv_client()
                .put(data_key, value, None)
                .await
                .map_err(|e| Error::StorageMsg(e.to_string()))?;
        }
        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        for (key, row) in rows {
            let data_key = self.data_key(table_name, &key)?;
            let value =
                serde_json::to_string(&row).map_err(|e| Error::StorageMsg(e.to_string()))?;
            self.client
                .kv_client()
                .put(data_key, value, None)
                .await
                .map_err(|e| Error::StorageMsg(e.to_string()))?;
        }
        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        for key in keys {
            let data_key = self.data_key(table_name, &key)?;
            self.client
                .kv_client()
                .delete(data_key, None)
                .await
                .map_err(|e| Error::StorageMsg(e.to_string()))?;
        }
        Ok(())
    }
}
