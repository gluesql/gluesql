use {
    crate::{
        ParquetStorage,
        error::{OptionExt, ParquetStorageError, ResultExt},
    },
    async_trait::async_trait,
    futures::stream::iter,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, RowIter, Store},
    },
    std::{ffi::OsStr, fs},
};

#[async_trait(?Send)]
impl Store for ParquetStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.fetch_schema(table_name)
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let paths = fs::read_dir(&self.path).map_storage_err()?;
        let mut schemas = paths
            .map(|result| {
                let path = result.map_storage_err()?.path();

                let table_name = path
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .map_storage_err(ParquetStorageError::FileNotFound)?;

                self.fetch_schema(table_name)?
                    .map_storage_err(ParquetStorageError::TableDoesNotExist(
                        table_name.to_owned(),
                    ))
                    .map(Some)
            })
            .filter_map(Result::transpose)
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
        return Ok(None);
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let rows = self.scan_data(table_name)?.0;
        Ok(Box::pin(iter(rows)))
    }
}
