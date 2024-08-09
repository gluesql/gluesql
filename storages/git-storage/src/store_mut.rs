use {
    crate::{GitStorage, StorageBase},
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, StoreMut},
    },
};

#[async_trait(?Send)]
impl StoreMut for GitStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        match &mut self.storage_base {
            StorageBase::File(storage) => storage.insert_schema(schema).await,
        }
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        match &mut self.storage_base {
            StorageBase::File(storage) => storage.delete_schema(table_name).await,
        }
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        match &mut self.storage_base {
            StorageBase::File(storage) => storage.append_data(table_name, rows).await,
        }
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        match &mut self.storage_base {
            StorageBase::File(storage) => storage.insert_data(table_name, rows).await,
        }
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        match &mut self.storage_base {
            StorageBase::File(storage) => storage.delete_data(table_name, keys).await,
        }
    }
}
