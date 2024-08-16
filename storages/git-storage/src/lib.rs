#![deny(clippy::str_to_string)]

mod store;
mod store_mut;

use {
    gluesql_core::{
        error::{Error, Result},
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, FileBased, Index, IndexMut, Metadata,
            Transaction,
        },
    },
    gluesql_csv_storage::CsvStorage,
    gluesql_file_storage::FileStorage,
    gluesql_json_storage::JsonStorage,
    std::process::Command,
    strum_macros::Display,
};

pub struct GitStorage<T: FileBased> {
    pub storage_base: T,
    pub path: String,
    pub remote: String,
    pub branch: String,
}

pub enum StorageBase {
    File(FileStorage),
    Csv(CsvStorage),
    Json(JsonStorage),
}

#[derive(Clone, Copy, Display)]
#[strum(serialize_all = "lowercase")]
pub enum StorageType {
    File,
    Csv,
    Json,
}

const DEFAULT_REMOTE: &str = "origin";
const DEFAULT_BRANCH: &str = "main";

impl<T: FileBased> GitStorage<T> {
    pub fn init(path: &str) -> Result<Self> {
        let storage_base = T::new(path)?;
        Command::new("git")
            .current_dir(path)
            .arg("init")
            .output()
            .expect("failed to git init");

        Ok(Self {
            storage_base,
            path: path.to_owned(),
            remote: DEFAULT_REMOTE.to_owned(),
            branch: DEFAULT_BRANCH.to_owned(),
        })
    }

    pub fn open(path: &str) -> Result<Self> {
        let storage_base = T::new(path)?;

        Ok(Self {
            storage_base,
            path: path.to_owned(),
            remote: DEFAULT_REMOTE.to_owned(),
            branch: DEFAULT_BRANCH.to_owned(),
        })
    }

    pub fn set_remote(&mut self, remote: String) {
        self.remote = remote;
    }

    pub fn set_branch(&mut self, branch: String) {
        self.branch = branch;
    }

    pub fn add_and_commit(&self, message: &str) -> Result<()> {
        Command::new("git")
            .current_dir(&self.path)
            .arg("add")
            .arg(".")
            .output()
            .map_storage_err()?;

        Command::new("git")
            .current_dir(&self.path)
            .arg("commit")
            .arg("-m")
            .arg(message)
            .output()
            .map_storage_err()?;

        Ok(())
    }

    pub fn pull(&self) -> Result<()> {
        Command::new("git")
            .current_dir(&self.path)
            .arg("pull")
            .arg(&self.remote)
            .arg(&self.branch)
            .output()
            .map_storage_err()
            .map(|_| ())
    }

    pub fn push(&self) -> Result<()> {
        Command::new("git")
            .current_dir(&self.path)
            .arg("push")
            .arg(&self.remote)
            .arg(&self.branch)
            .output()
            .map_storage_err()
            .map(|_| ())
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

impl<T: FileBased> AlterTable for GitStorage<T> {}
impl<T: FileBased> Index for GitStorage<T> {}
impl<T: FileBased> IndexMut for GitStorage<T> {}
impl<T: FileBased> Transaction for GitStorage<T> {}
impl<T: FileBased> Metadata for GitStorage<T> {}
impl<T: FileBased> CustomFunction for GitStorage<T> {}
impl<T: FileBased> CustomFunctionMut for GitStorage<T> {}
