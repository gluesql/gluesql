use {
    crate::{
        ParquetStorage,
        column_def::ParquetSchemaType,
        error::{OptionExt, ParquetStorageError, ResultExt},
    },
    gluesql_core::{
        ast::{ColumnDef, ForeignKey},
        data::{Key, Schema},
        error::{Error, Result},
        prelude::Value,
        store::{RowIter, Store},
    },
    parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader},
    serde_json::from_str,
    std::{
        ffi::OsStr,
        fs::{self, File},
    },
};

impl Store for ParquetStorage {
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let schema_path = self.data_path(table_name);
        let is_schema_path_exist = schema_path.exists();
        if !is_schema_path_exist {
            return Ok(None);
        }
        let file = File::open(&schema_path).map_storage_err()?;
        let reader = SerializedFileReader::new(file).map_storage_err()?;
        let parquet_metadata = reader.metadata();
        let file_metadata = parquet_metadata.file_metadata();
        let schema = file_metadata.schema();
        let key_value_file_metadata = file_metadata.key_value_metadata();

        let mut is_schemaless = false;
        let mut foreign_keys = Vec::new();
        let mut comment = None;
        if let Some(metadata) = key_value_file_metadata {
            for kv in metadata {
                if kv.key == "schemaless" {
                    is_schemaless = matches!(kv.value.as_deref(), Some("true"));
                } else if kv.key == "comment" {
                    comment.clone_from(&kv.value);
                } else if kv.key.starts_with("foreign_key") {
                    let fk = kv
                        .value
                        .as_ref()
                        .map(|x| from_str::<ForeignKey>(x))
                        .map_storage_err(Error::StorageMsg(
                            "No value found on metadata".to_owned(),
                        ))?
                        .map_storage_err()?;

                    foreign_keys.push(fk);
                }
            }
        }

        let column_defs = if is_schemaless {
            None
        } else {
            Some(
                schema
                    .get_fields()
                    .iter()
                    .map(|field| {
                        ColumnDef::try_from(ParquetSchemaType {
                            inner: field,
                            metadata: key_value_file_metadata,
                        })
                    })
                    .collect::<Result<Vec<ColumnDef>, _>>()?,
            )
        };

        Ok(Some(Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes: vec![],
            engine: None,
            foreign_keys,
            comment,
        }))
    }

    fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
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

    fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<Vec<Value>>> {
        for item in self.scan_data(table_name)?.0 {
            let (key, row) = item?;

            if &key == target {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let rows = self.scan_data(table_name)?.0;
        Ok(Box::new(rows))
    }
}
