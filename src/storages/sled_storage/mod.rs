mod alter_table;
mod error;
mod gc;
mod index;
mod index_mut;
mod index_sync;
mod key;
mod lock;
mod snapshot;
mod store;
mod store_mut;
mod transaction;

use {
    self::snapshot::Snapshot,
    crate::{
        store::{GStore, GStoreMut},
        Error, Result,
    },
    error::err_into,
    sled::{
        self,
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
        },
        Config, Db, IVec,
    },
    std::convert::TryFrom,
};

#[derive(Debug, Clone)]
pub enum State {
    Idle,
    Transaction { txid: u64, autocommit: bool },
}

#[derive(Debug, Clone)]
pub struct SledStorage {
    pub tree: Db,
    pub state: State,
}

impl SledStorage {
    pub fn new(filename: &str) -> Result<Self> {
        let tree = sled::open(filename).map_err(err_into)?;
        let state = State::Idle;

        Ok(Self { tree, state })
    }
}

impl TryFrom<Config> for SledStorage {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        let tree = config.open().map_err(err_into)?;
        let state = State::Idle;

        Ok(Self { tree, state })
    }
}

fn fetch_schema(
    tree: &TransactionalTree,
    table_name: &str,
) -> ConflictableTransactionResult<(String, Option<Snapshot<crate::data::Schema>>), Error> {
    let key = format!("schema/{}", table_name);
    let value = tree.get(&key.as_bytes())?;
    let schema_snapshot = value
        .map(|v| bincode::deserialize(&v))
        .transpose()
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?;

    Ok((key, schema_snapshot))
}

impl GStore<IVec> for SledStorage {}
impl GStoreMut<IVec> for SledStorage {}
