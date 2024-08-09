use {
    crate::{FileRow, FileStorage, ResultExt},
    async_trait::async_trait,
    futures::stream::iter,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, RowIter, Store},
    },
    std::fs,
};

#[async_trait(?Send)]
impl Store for FileStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut schemas = fs::read_dir(&self.path)
            .map_storage_err()?
            .map(|dir_entry| {
                let dir_entry = dir_entry.map_storage_err()?;
                let file_type = dir_entry.file_type().map_storage_err()?;
                if file_type.is_dir() {
                    return Ok(None);
                }

                let path = dir_entry.path();
                fs::read_to_string(path)
                    .map_storage_err()
                    .and_then(|data| Schema::from_ddl(&data))
                    .map(Some)
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

        fs::read_to_string(path)
            .map_storage_err()
            .and_then(|data| Schema::from_ddl(&data))
            .map(Some)
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
        let mut rows = fs::read_dir(path)
            .map_storage_err()?
            .map(|dir_entry| {
                let dir_entry = dir_entry.map_storage_err()?;
                let file_type = dir_entry.file_type().map_storage_err()?;
                if file_type.is_dir() {
                    return Ok(None);
                }

                let path = dir_entry.path();
                fs::read_to_string(&path)
                    .map_storage_err()
                    .and_then(move |data| {
                        let FileRow { key, row } = ron::from_str(&data).map_storage_err()?;

                        Ok((path, key, row))
                    })
                    .map(Some)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>>>()?;

        rows.sort_by_cached_key(|(path, _, _)| path.clone());
        let rows = rows.into_iter().map(|(_, key, row)| (key, row)).map(Ok);

        Ok(Box::pin(iter(rows)))
    }
}
