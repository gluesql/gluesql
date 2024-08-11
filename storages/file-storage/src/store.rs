use {
    crate::{FileRow, FileStorage, ResultExt},
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
impl Store for FileStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut schemas = fs::read_dir(&self.path)
            .map_storage_err()?
            .map(|dir_entry| {
                let dir_entry = dir_entry.map_storage_err()?;
                let file_type = dir_entry.file_type().map_storage_err()?;
                let path = dir_entry.path();
                let extension = path.extension().and_then(OsStr::to_str);
                if file_type.is_dir() || extension != Some("sql") {
                    return Ok(None);
                }

                self.fetch_schema(path).map(Some)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<Schema>>>()?;

        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let path = self.path(table_name).with_extension("sql");
        if !path.exists() {
            return Ok(None);
        }

        self.fetch_schema(path).map(Some)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        let path = self.data_path(table_name, key)?;
        if !path.exists() {
            return Ok(None);
        }

        fs::read_to_string(path)
            .map_storage_err()
            .and_then(|data| {
                ron::from_str(&data)
                    .map_storage_err()
                    .map(|FileRow { row, .. }| row)
            })
            .map(Some)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let path = self.path(table_name);
        let mut entries = fs::read_dir(path)
            .map_storage_err()?
            .map(|dir_entry| {
                let dir_entry = dir_entry.map_storage_err()?;
                let file_type = dir_entry.file_type().map_storage_err()?;
                let path = dir_entry.path();
                let extension = path.extension().and_then(OsStr::to_str);
                if file_type.is_dir() || extension != Some("ron") {
                    return Ok(None);
                }

                Ok(Some(path))
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>>>()?;

        entries.sort();

        let rows = entries.into_iter().map(|path| {
            let data = fs::read_to_string(path).map_storage_err()?;

            ron::from_str(&data)
                .map_storage_err()
                .map(|FileRow { key, row }| (key, row))
        });

        Ok(Box::pin(iter(rows)))
    }
}
