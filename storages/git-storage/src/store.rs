use {
    crate::GitStorage,
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
        self.get_store().fetch_all_schemas().await
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.get_store().fetch_schema(table_name).await
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        self.get_store().fetch_data(table_name, key).await
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        self.get_store().scan_data(table_name).await
    }
}
