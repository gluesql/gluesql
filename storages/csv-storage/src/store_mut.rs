use {
    crate::{csv_table::CsvTable, error::StorageError, CsvStorage},
    async_trait::async_trait,
    gluesql_core::{
        data::{schema::Schema, Key},
        result::Result,
        store::{DataRow, StoreMut},
    },
    std::{fs, path::PathBuf},
};

#[async_trait(?Send)]
impl StoreMut for CsvStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let file_path = PathBuf::from(format!("{}.csv", schema.table_name));

        match fs::File::create(&file_path) {
            Ok(_) => {
                let csv_table = CsvTable {
                    file_path,
                    schema: schema.to_owned(),
                };
                let mut tables = self.tables;
                tables.insert(schema.table_name.to_owned(), csv_table);

                Ok(())
            }
            Err(_) => Err(StorageError::FailedToCreateTableFile.into()),
        }
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let mut tables = self.tables;
        match tables.remove(table_name) {
            Some(_) => Ok(()),
            None => Err(StorageError::TableNotFound(table_name.to_string()).into()),
        }
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        todo!()
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        todo!()
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        todo!()
    }
}
