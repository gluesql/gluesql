use {
    crate::GitStorage,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::StoreMut,
    },
};

impl StoreMut for GitStorage {
    fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        self.get_store_mut().insert_schema(schema)?;

        self.add_and_commit(&format!(
            "[GitStorage::insert_schema] {}",
            schema.table_name
        ))
    }

    fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.get_store_mut().delete_schema(table_name)?;

        self.add_and_commit("[GitStorage::delete_schema] {table_name}")
    }

    fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        let n = rows.len();
        if n == 0 {
            return Ok(());
        }

        self.get_store_mut().append_data(table_name, rows)?;

        self.add_and_commit(&format!(
            "[GitStorage::append_data] {table_name} - {n} rows"
        ))
    }

    fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        let n = rows.len();
        if n == 0 {
            return Ok(());
        }

        self.get_store_mut().insert_data(table_name, rows)?;

        self.add_and_commit(&format!(
            "[GitStorage::insert_data] {table_name} - {n} rows"
        ))
    }

    fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let n = keys.len();
        if n == 0 {
            return Ok(());
        }

        self.get_store_mut().delete_data(table_name, keys)?;

        self.add_and_commit(&format!(
            "[GitStorage::delete_data] {table_name} - {n} rows"
        ))
    }
}
