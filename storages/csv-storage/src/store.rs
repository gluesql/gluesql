use {
    crate::{
        CsvStorage,
        error::{CsvStorageError, OptionExt, ResultExt},
    },
    async_trait::async_trait,
    futures::stream::iter,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::{RowIter, Store},
    },
    std::{ffi::OsStr, fs},
};

#[async_trait]
impl Store for CsvStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.fetch_schema(table_name)
            .map(|schema| schema.map(|(schema, _)| schema))
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let paths = fs::read_dir(&self.path).map_storage_err()?;
        let mut schemas = paths
            .map(|result| {
                let path = result.map_storage_err()?.path();
                let extension = path.extension().and_then(OsStr::to_str);
                if extension != Some("csv") || path.to_string_lossy().ends_with(".types.csv") {
                    return Ok(None);
                }

                let table_name = path
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .map_storage_err(CsvStorageError::FileNotFound)?;

                self.fetch_schema(table_name)?
                    .map(|(schema, _)| schema)
                    .map_storage_err(CsvStorageError::TableDoesNotExist)
                    .map(Some)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<Schema>>>()?;

        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<Vec<Value>>> {
        let (_, rows) = self.scan_data(table_name)?;

        for item in rows {
            let (key, row) = item?;

            if &key == target {
                return Ok(Some(row));
            }
        }

        Ok(None)
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let rows = self.scan_data(table_name).map(|(_, rows)| rows)?;

        Ok(Box::pin(iter(rows)))
    }
}
