use {
    crate::CsvStorage,
    async_trait::async_trait,
    gluesql_core::{data::Schema, prelude::*, result::Result, store::RowIter, store::Store},
};

#[async_trait(?Send)]
impl Store for CsvStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let result = self
            .schema_list
            .iter()
            .find(|schema| schema.table_name.eq(table_name))
            .map(ToOwned::to_owned);
        Ok(result)
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        Ok(self.schema_list.clone())
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Row>> {
        todo!()
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        todo!()
    }
}
