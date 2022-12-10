use {
    crate::CsvStorage,
    async_trait::async_trait,
    gluesql_core::{
        data::{schema::Schema, Key},
        result::MutResult,
        store::Row,
        store::StoreMut,
    },
};

#[async_trait(?Send)]
impl StoreMut for CsvStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        todo!()
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        todo!()
    }

    async fn append_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        todo!()
    }

    async fn insert_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        todo!()
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        todo!()
    }
}
