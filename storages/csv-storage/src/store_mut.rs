use {
    crate::{csv_table::CsvTable, error::StorageError, CsvStorage},
    async_trait::async_trait,
    gluesql_core::{
        data::{schema::Schema, Key},
        result::MutResult,
        store::Row,
        store::StoreMut,
    },
    std::{fs, path::PathBuf},
};

#[async_trait(?Send)]
impl StoreMut for CsvStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let file_path = PathBuf::from(format!("{}.csv", schema.table_name));

        match fs::File::create(&file_path) {
            Ok(_) => {
                let csv_table = CsvTable {
                    file_path,
                    schema: schema.to_owned(),
                };
                let mut tables = self.tables;
                tables.insert(schema.table_name.to_owned(), csv_table);

                Ok((CsvStorage { tables }, ()))
            }
            Err(_) => Err((self, StorageError::FailedToCreateTableFile.into())),
        }
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
