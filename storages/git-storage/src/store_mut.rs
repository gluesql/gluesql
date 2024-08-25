use {
    crate::GitStorage,
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
        self.get_store_mut().insert_schema(schema).await?;

        self.add_and_commit(&format!(
            "[GitStorage::insert_schema] {}",
            schema.table_name
        ))
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.get_store_mut().delete_schema(table_name).await?;

        self.add_and_commit("[GitStorage::delete_schema] {table_name}")
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        let n = rows.len();
        if n == 0 {
            return Ok(());
        }

        self.get_store_mut().append_data(table_name, rows).await?;

        self.add_and_commit(&format!(
            "[GitStorage::append_data] {table_name} - {n} rows"
        ))
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        let n = rows.len();
        if n == 0 {
            return Ok(());
        }

        self.get_store_mut().insert_data(table_name, rows).await?;

        self.add_and_commit(&format!(
            "[GitStorage::insert_data] {table_name} - {n} rows"
        ))
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let n = keys.len();
        if n == 0 {
            return Ok(());
        }

        self.get_store_mut().delete_data(table_name, keys).await?;

        self.add_and_commit(&format!(
            "[GitStorage::delete_data] {table_name} - {n} rows"
        ))
    }
}
