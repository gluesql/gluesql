use {
    crate::{
        JsonStorage,
        error::{JsonStorageError, OptionExt, ResultExt},
    },
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{Error, Result},
        store::{RowIter, Store},
    },
    std::{
        ffi::OsStr,
        fs::{self, File},
        io::Read,
    },
};

impl Store for JsonStorage {
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        match (
            self.jsonl_path(table_name).exists(),
            self.json_path(table_name).exists(),
        ) {
            (true, true) => {
                return Err(Error::StorageMsg(
                    JsonStorageError::BothJsonlAndJsonExist(table_name.to_owned()).to_string(),
                ));
            }
            (false, false) => return Ok(None),
            _ => {}
        }

        let schema_path = self.schema_path(table_name);
        let (column_defs, foreign_keys, comment) = if schema_path.exists() {
            let mut file = File::open(&schema_path).map_storage_err()?;
            let mut ddl = String::new();
            file.read_to_string(&mut ddl).map_storage_err()?;

            let schema = Schema::from_ddl(&ddl)?;
            if schema.table_name != table_name {
                return Err(Error::StorageMsg(
                    JsonStorageError::TableNameDoesNotMatchWithFile.to_string(),
                ));
            }

            (schema.column_defs, schema.foreign_keys, schema.comment)
        } else {
            (None, Vec::new(), None)
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
                let extension = path.extension().and_then(OsStr::to_str);
                if extension != Some("jsonl") && extension != Some("json") {
                    return Ok(None);
                }

                let table_name = path
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .map_storage_err(JsonStorageError::FileNotFound)?;

                self.fetch_schema(table_name)?
                    .map_storage_err(JsonStorageError::TableDoesNotExist)
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
