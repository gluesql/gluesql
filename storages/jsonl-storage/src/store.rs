use {
    crate::error::{JsonlStorageError, OptionExt, ResultExt},
    crate::JsonlStorage,
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
impl Store for JsonlStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.fetch_schema(table_name)
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let paths = fs::read_dir(&self.path).map_storage_err()?;
        let mut schemas = paths
            .filter(|result| {
                result
                    .as_ref()
                    .map(|dir_entry| {
                        dir_entry
                            .path()
                            .extension()
                            .map(|os_str| os_str.to_str() == Some("jsonl"))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false)
            })
            .map(|result| -> Result<_> {
                let path = result.map_storage_err()?.path();
                let table_name = path
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .map_storage_err(JsonlStorageError::FileNotFound)?;

                self.fetch_schema(table_name)?
                    .map_storage_err(JsonlStorageError::TableDoesNotExist)
            })
            .collect::<Result<Vec<Schema>>>()?;

        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        self.scan_data(table_name)?
            .find_map(|result| {
                result
                    .map(|(key, row)| (&key == target).then_some(row))
                    .transpose()
            })
            .transpose()
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        self.scan_data(table_name)
    }
}
