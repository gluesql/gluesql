#![deny(clippy::str_to_string)]

mod store;
mod store_mut;

use {
    std::process::Command,
    git2::{IndexAddOption, Repository, Signature},
    gluesql_core::{
        error::{Error, Result},
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, Transaction,
        },
    },
    gluesql_csv_storage::CsvStorage,
    // gluesql_file_storage::FileStorage,
    gluesql_json_storage::JsonStorage,
    strum_macros::Display,
};

pub use git2;

pub struct GitStorage {
    pub storage_base: StorageBase,
    /*
    pub repo: Repository,
    pub signature: Signature<'static>,
    */
    pub path: String,
}

pub enum StorageBase {
    // File(FileStorage),
    Csv(CsvStorage),
    Json(JsonStorage),
}

#[derive(Clone, Copy, Display)]
#[strum(serialize_all = "lowercase")]
pub enum StorageType {
    // File,
    Csv,
    Json,
}

fn signature() -> Result<Signature<'static>> {
    Signature::now("GlueSQL Bot", "bot.glue.glue.gluesql@gluesql.org").map_storage_err()
}

impl GitStorage {
    pub fn init(path: &str, storage_type: StorageType) -> Result<Self> {
        let storage_base = Self::storage_base(path, storage_type)?;
        /*
        let repo = Repository::init(path).map_storage_err()?;
        let signature = signature()?;
        */

        Command::new("git")
            .current_dir(path)
            .arg("init")
            .output()
            .expect("failed to git init");

        Ok(Self {
            storage_base,
            path: path.to_owned(),
            /*
            repo,
            signature,
            */
        })
    }

    /*
    pub fn with_repo(repo: Repository, storage_type: StorageType) -> Result<Self> {
        let path = repo.path().to_str().map_storage_err("path not exists")?;
        let storage_base = Self::storage_base(path, storage_type)?;
        let signature = signature()?;

        Ok(Self {
            storage_base,
            repo,
            signature,
        })
    }
    */

    fn storage_base(path: &str, storage_type: StorageType) -> Result<StorageBase> {
        use StorageType::*;

        match storage_type {
            // File => FileStorage::new(path).map(StorageBase::File),
            Csv => CsvStorage::new(path).map(StorageBase::Csv),
            Json => JsonStorage::new(path).map(StorageBase::Json),
        }
    }

    /*
    pub fn set_signature(&mut self, signature: Signature<'static>) {
        self.signature = signature;
    }
    */

    pub fn pull(&self) -> Result<()> {
        todo!()
    }

    pub fn push(&self) -> Result<()> {
        todo!()
    }

    pub fn add_and_commit(&self, message: &str) -> Result<()> {
        Command::new("git")
            .arg("add")
            .arg(".")
            .output()
            .unwrap();

        Command::new("git")
            .arg("commit")
            .arg("-m")
            .arg(message)
            .output()
            .unwrap();
        /*
        (|| -> std::result::Result<(), git2::Error> {
            let mut index = self.repo.index()?;

            index.add_all(["*"].iter(), IndexAddOption::DEFAULT, None)?;
            index.write()?;

            let tree_id = index.write_tree_to(&self.repo)?;
            let tree = self.repo.find_tree(tree_id)?;
            let parent_commit = match self.repo.head() {
                Ok(head) => vec![head.resolve()?.peel_to_commit()?],
                Err(_) => vec![],
            };

            self.repo.commit(
                Some("HEAD"),
                &self.signature,
                &self.signature,
                message,
                &tree,
                &parent_commit.iter().collect::<Vec<_>>(),
            )?;

            Ok(())
        })()
        .map_storage_err()?;
        */

        Ok(())
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

pub trait OptionExt<T> {
    fn map_storage_err(self, message: &str) -> Result<T, Error>;
}

impl<T> OptionExt<T> for std::option::Option<T> {
    fn map_storage_err(self, message: &str) -> Result<T, Error> {
        self.ok_or_else(|| Error::StorageMsg(message.to_owned()))
    }
}

impl AlterTable for GitStorage {}
impl Index for GitStorage {}
impl IndexMut for GitStorage {}
impl Transaction for GitStorage {}
impl Metadata for GitStorage {}
impl CustomFunction for GitStorage {}
impl CustomFunctionMut for GitStorage {}
