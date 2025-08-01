use {
    crate::{
        ParquetStorage,
        error::{OptionExt, ParquetStorageError, ResultExt},
    },
    async_trait::async_trait,
    futures::stream::iter,
    gluesql_core::{
        ast::ColumnUniqueOption,
        data::{Key, Schema},
        error::Result,
        store::{DataRow, RowIter, Store},
    },
    parquet::{
        file::{reader::FileReader, serialized_reader::SerializedFileReader},
        record::Row,
    },
    std::{
        ffi::OsStr,
        fs::{self, File},
    },
};

use crate::value::ParquetField;

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

    async fn scan_data_with_columns<'a>(
        &'a self,
        table_name: &str,
        columns: &[String],
    ) -> Result<RowIter<'a>> {
        use parquet::schema::types::TypePtr;

        let fetched_schema = self.fetch_schema(table_name)?.map_storage_err(
            ParquetStorageError::TableDoesNotExist(table_name.to_owned()),
        )?;

        if fetched_schema.column_defs.is_none() {
            return Store::scan_data_with_columns(self, table_name, columns).await;
        }

        let file = File::open(self.data_path(table_name)).map_storage_err()?;
        let parquet_reader = SerializedFileReader::new(file).map_storage_err()?;
        let file_schema = parquet_reader.metadata().file_metadata().schema();

        let mut fields: Vec<TypePtr> = Vec::new();
        let mut proj_defs = Vec::new();
        if let Some(column_defs) = &fetched_schema.column_defs {
            for (idx, field) in file_schema.get_fields().iter().enumerate() {
                if columns.iter().any(|c| c == field.name()) {
                    fields.push(field.clone());
                    proj_defs.push(column_defs[idx].clone());
                }
            }
        }

        let proj_schema_type = parquet::schema::types::Type::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .map_storage_err()?;

        let row_iter = parquet_reader
            .get_row_iter(Some(proj_schema_type))
            .map_storage_err()?;

        let proj_schema = Schema {
            table_name: fetched_schema.table_name,
            column_defs: Some(proj_defs),
            indexes: vec![],
            engine: None,
            foreign_keys: vec![],
            comment: None,
        };

        let mut rows = Vec::new();
        let mut key_counter: u64 = 0;
        for record in row_iter {
            let record: Row = record.map_storage_err()?;
            let mut row = Vec::new();
            let mut key = None;

            for (idx, (_, field)) in record.get_column_iter().enumerate() {
                let value = ParquetField(field.clone()).to_value(&proj_schema, idx)?;
                if let Some(column_defs) = &proj_schema.column_defs {
                    if column_defs[idx].unique == Some(ColumnUniqueOption { is_primary: true }) {
                        key = Key::try_from(&value).ok();
                    }
                }
                row.push(value);
            }

            let generated_key = key.unwrap_or_else(|| {
                let generated = Key::U64(key_counter);
                key_counter += 1;
                generated
            });

            rows.push(Ok((generated_key, DataRow::Vec(row))));
        }

        Ok(Box::pin(iter(rows)))
    }
}
