use {
    crate::{FileRow, FileStorage, ResultExt},
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::StoreMut,
    },
    ron::ser::{PrettyConfig, to_string_pretty},
    std::{
        fs::{self, File},
        io::Write,
    },
    uuid::Uuid,
};

#[async_trait]
impl StoreMut for FileStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let table_name = schema.table_name.clone();
        let path = self.path(table_name);
        if !path.exists() {
            fs::create_dir(&path).map_storage_err()?;
        }

        let path = path.with_extension("sql");
        Self::write_schema_file(&path, schema)?;

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let path = self.path(table_name);
        if !path.exists() {
            return Ok(());
        }

        fs::remove_dir_all(&path).map_storage_err()?;

        let path = path.with_extension("sql");
        fs::remove_file(path).map_storage_err()?;

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        for row in rows {
            let key = Key::Uuid(Uuid::now_v7().as_u128());
            let path = self.data_path(table_name, &key)?;
            let row = FileRow { key, row };
            let row = to_string_pretty(&row, PrettyConfig::default()).map_storage_err()?;

            let mut file = File::create(path).map_storage_err()?;
            file.write_all(row.as_bytes()).map_storage_err()?;
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        for (key, row) in rows {
            let path = self.data_path(table_name, &key)?;
            let row = FileRow { key, row };
            let row = to_string_pretty(&row, PrettyConfig::default()).map_storage_err()?;

            let mut file = File::create(path).map_storage_err()?;
            file.write_all(row.as_bytes()).map_storage_err()?;
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        for key in keys {
            let path = self.data_path(table_name, &key)?;

            fs::remove_file(path).map_storage_err()?;
        }

        Ok(())
    }
}
