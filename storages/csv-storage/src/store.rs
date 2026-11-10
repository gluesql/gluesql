use {
    crate::{
        CsvStorage,
        error::{CsvStorageError, OptionExt, ResultExt},
    },
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::{RowIter, ScanColumns, Store},
    },
    std::{ffi::OsStr, fs},
};

impl Store for CsvStorage {
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.fetch_schema(table_name)
            .map(|schema| schema.map(|(schema, _)| schema))
    }

    fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
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

    fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<Vec<Value>>> {
        let (_, rows) = self.scan_data(table_name)?;

        for item in rows {
            let (key, row) = item?;

            if &key == target {
                return Ok(Some(row));
            }
        }

        Ok(None)
    }

    fn scan_data<'a>(&'a self, table_name: &str, columns: ScanColumns<'_>) -> Result<RowIter<'a>> {
        let rows = self.scan_data(table_name).map(|(_, rows)| rows)?;
        let ScanColumns::Selected(requested_columns) = columns else {
            return Ok(rows);
        };

        let column_indexes = requested_columns
            .iter()
            .map(|(_, index)| *index)
            .collect::<Vec<_>>();

        let rows = rows.map(move |row| {
            let (key, row) = row?;
            let mut selected_indexes = column_indexes.iter().copied().peekable();
            let row = row
                .into_iter()
                .enumerate()
                .filter_map(|(index, value)| selected_indexes.next_if_eq(&index).map(|_| value))
                .collect();

            Ok((key, row))
        });

        Ok(Box::new(rows))
    }
}
