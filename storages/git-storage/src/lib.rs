#![deny(clippy::str_to_string)]

mod store;
mod store_mut;

use {
    gluesql_core::{
        error::Result,
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, Transaction,
        },
    },
    gluesql_file_storage::FileStorage,
    std::path::PathBuf,
};

#[derive(Debug, Clone)]
pub struct GitStorage {
    pub storage_base: StorageBase,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub enum StorageBase {
    File(FileStorage),
    /*
    Csv(CsvStorage),
    Json(JsonStorage),
    Parquet(ParquetStorage),
    */
}

impl GitStorage {
    pub fn new(path: &str) -> Result<Self> {
        let storage = FileStorage::new(path)?;
        let storage_base = StorageBase::File(storage);
        let path = PathBuf::from(path);

        Ok(Self { path, storage_base })
    }

    /*
    pub fn path<T: AsRef<Path>>(&self, table_name: T) -> PathBuf {
        let mut path = self.path.clone();
        path.push(table_name);
        path
    }

    pub fn data_path<T: AsRef<Path>>(&self, table_name: T, key: &Key) -> Result<PathBuf> {
        let mut path = self.path(table_name);
        let key = key.to_cmp_be_bytes()?.encode_hex::<String>();

        path.push(key);
        let path = path.with_extension("ron");

        Ok(path)
    }
    */
}

/*
pub trait ResultExt<T, E: ToString> {
    fn map_storage_err(self) -> Result<T, Error>;
}

impl<T, E: ToString> ResultExt<T, E> for std::result::Result<T, E> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}
*/

impl AlterTable for GitStorage {}
impl Index for GitStorage {}
impl IndexMut for GitStorage {}
impl Transaction for GitStorage {}
impl Metadata for GitStorage {}
impl CustomFunction for GitStorage {}
impl CustomFunctionMut for GitStorage {}
