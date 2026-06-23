use {
    crate::GitStorage,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::{RowIter, Store},
    },
};

impl Store for GitStorage {
    fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        self.get_store().fetch_all_schemas()
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.get_store().fetch_schema(table_name)
    }

    fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        self.get_store().fetch_data(table_name, key)
    }

    fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        self.get_store().scan_data(table_name)
    }
}
