use {
    crate::{
        error::{JsonStorageError, OptionExt, ResultExt},
        JsonStorage,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::Schema,
        prelude::Key,
        result::Result,
        store::{DataRow, RowIter, Store},
    },
    std::{ffi::OsStr, fs},
};

#[async_trait(?Send)]
impl Store for JsonStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.fetch_schema(table_name)
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let paths = fs::read_dir(&self.path).map_storage_err()?;
        let mut schemas = paths
            .filter(|result| match result {
                Ok(entry) => {
                    let path = entry.path();
                    let extension = path.extension().and_then(OsStr::to_str);

                    extension == Some("jsonl") || extension == Some("json")
                }
                Err(_) => true,
            })
            .map(|result| -> Result<_> {
                let path = result.map_storage_err()?.path();
                let table_name = path
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .map_storage_err(JsonStorageError::FileNotFound)?;

                self.fetch_schema(table_name)?
                    .map_storage_err(JsonStorageError::TableDoesNotExist)
            })
            .collect::<Result<Vec<Schema>>>()?;

        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        for item in self.scan_data(table_name)?.0 {
            let (key, row) = item?;

            if &key == target {
                return Ok(Some(row));
            }
        }

        Ok(None)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        Ok(self.scan_data(table_name)?.0)
    }
}
