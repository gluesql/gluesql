#![deny(clippy::str_to_string)]

mod store;
mod store_mut;

use {
    gluesql_core::{
        data::Key,
        error::{Error, Result},
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata,
            Transaction,
        },
    },
    hex::ToHex,
    serde::{Deserialize, Serialize},
    std::{
        convert::AsRef,
        fs,
        path::{Path, PathBuf},
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStorage {
    pub path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileRow {
    pub key: Key,
    pub row: DataRow,
}

impl FileStorage {
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let path = PathBuf::from(path);

        Ok(Self { path })
    }

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
}

pub trait ResultExt<T, E: ToString> {
    fn map_storage_err(self) -> Result<T, Error>;
}

impl<T, E: ToString> ResultExt<T, E> for std::result::Result<T, E> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl AlterTable for FileStorage {}
impl Index for FileStorage {}
impl IndexMut for FileStorage {}
impl Transaction for FileStorage {}
impl Metadata for FileStorage {}
impl CustomFunction for FileStorage {}
impl CustomFunctionMut for FileStorage {}
