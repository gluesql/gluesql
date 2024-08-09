#![deny(clippy::str_to_string)]

mod store;
mod store_mut;

use {
    git2::{IndexAddOption, Repository, Signature},
    gluesql_core::{
        error::{Error, Result},
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, Transaction,
        },
    },
    gluesql_file_storage::FileStorage,
};

pub struct GitStorage {
    pub storage_base: StorageBase,
    pub repo: Repository,
    pub signature: Signature<'static>,
}

pub enum StorageBase {
    File(FileStorage),
    /*
    Csv(CsvStorage),
    Json(JsonStorage),
    Parquet(ParquetStorage),
    */
}

fn signature() -> Result<Signature<'static>> {
    Signature::now("GlueSQL Bot", "bot.glue.glue.gluesql@gluesql.org").map_storage_err()
}

impl GitStorage {
    pub fn init(path: &str) -> Result<Self> {
        let storage = FileStorage::new(path)?;
        let storage_base = StorageBase::File(storage);
        let repo = Repository::init(path).map_storage_err()?;
        let signature = signature()?;

        Ok(Self {
            storage_base,
            repo,
            signature,
        })
    }

    pub fn open(path: &str) -> Result<Self> {
        let storage = FileStorage::new(path)?;
        let storage_base = StorageBase::File(storage);
        let repo = Repository::open(path).map_storage_err()?;
        let signature = signature()?;

        Ok(Self {
            storage_base,
            repo,
            signature,
        })
    }

    pub fn pull(&self) -> Result<()> {
        todo!()
    }

    pub fn push(&self) -> Result<()> {
        todo!()
    }

    pub fn add_and_commit(&self, message: &str) -> Result<()> {
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

impl AlterTable for GitStorage {}
impl Index for GitStorage {}
impl IndexMut for GitStorage {}
impl Transaction for GitStorage {}
impl Metadata for GitStorage {}
impl CustomFunction for GitStorage {}
impl CustomFunctionMut for GitStorage {}
