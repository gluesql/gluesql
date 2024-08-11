use {
    crate::{GitStorage, StorageBase},
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, RowIter, Store},
    },
};

#[async_trait(?Send)]
impl Store for GitStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        match &self.storage_base {
            StorageBase::File(storage) => storage.fetch_all_schemas().await,
            StorageBase::Csv(storage) => storage.fetch_all_schemas().await,
            StorageBase::Json(storage) => storage.fetch_all_schemas().await,
        }
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        match &self.storage_base {
            StorageBase::File(storage) => storage.fetch_schema(table_name).await,
            StorageBase::Csv(storage) => storage.fetch_schema(table_name).await,
            StorageBase::Json(storage) => storage.fetch_schema(table_name).await,
        }
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        match &self.storage_base {
            StorageBase::File(storage) => storage.fetch_data(table_name, key).await,
            StorageBase::Csv(storage) => storage.fetch_data(table_name, key).await,
            StorageBase::Json(storage) => storage.fetch_data(table_name, key).await,
        }
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        match &self.storage_base {
            StorageBase::File(storage) => storage.scan_data(table_name).await,
            StorageBase::Csv(storage) => storage.scan_data(table_name).await,
            StorageBase::Json(storage) => storage.scan_data(table_name).await,
        }
    }
}
