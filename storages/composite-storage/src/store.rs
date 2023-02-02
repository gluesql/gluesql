use {
    super::{CompositeStorage, IStorage},
    async_trait::async_trait,
    futures::stream::{self, StreamExt, TryStreamExt},
    gluesql_core::{
        data::{Key, Schema},
        result::Result,
        store::{DataRow, RowIter, Store},
    },
    std::iter::empty,
};

#[async_trait(?Send)]
impl Store for CompositeStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let schemas = stream::iter(self.storages.values())
            .map(AsRef::as_ref)
            .then(<dyn IStorage>::fetch_all_schemas)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect();

        Ok(schemas)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        for storage in self.storages.values() {
            let schema = storage.fetch_schema(table_name).await?;

            if schema.is_some() {
                return Ok(schema);
            }
        }

        Ok(None)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        match self.fetch_storage(table_name).await? {
            Some(storage) => storage.fetch_data(table_name, key).await,
            None => Ok(None),
        }
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        match self.fetch_storage(table_name).await? {
            Some(storage) => storage.scan_data(table_name).await,
            None => Ok(Box::new(empty())),
        }
    }
}
