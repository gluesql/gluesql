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
        data::Schema,
        result::{Error, Result},
        store::{GStore, GStoreMut},
    },
    error::{err_into, tx_err_into},
    sled::{
        self,
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
        },
        Config, Db, IVec,
    },
};

/// default transaction timeout : 1 hour
const DEFAULT_TX_TIMEOUT: u128 = 3600 * 1000;

#[derive(Debug, Clone)]
pub enum State {
    Idle,
    Transaction {
        txid: u64,
        created_at: u128,
        autocommit: bool,
    },
}

#[derive(Debug, Clone)]
pub struct SledStorage {
    pub tree: Db,
    pub state: State,
    /// transaction timeout in milliseconds
    pub tx_timeout: Option<u128>,
}

impl SledStorage {
    pub fn new(filename: &str) -> Result<Self> {
        let tree = sled::open(filename).map_err(err_into)?;
        let state = State::Idle;
        let tx_timeout = Some(DEFAULT_TX_TIMEOUT);

        Ok(Self {
            tree,
            state,
            tx_timeout,
        })
    }

    pub fn set_transaction_timeout(&mut self, tx_timeout: Option<u128>) {
        self.tx_timeout = tx_timeout;
    }

    fn update_state(self, state: State) -> Self {
        Self {
            tree: self.tree,
            state,
            tx_timeout: self.tx_timeout,
        }
    }
}

impl TryFrom<Config> for SledStorage {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        let tree = config.open().map_err(err_into)?;
        let state = State::Idle;
        let tx_timeout = Some(DEFAULT_TX_TIMEOUT);

        Ok(Self {
            tree,
            state,
            tx_timeout,
        })
    }
}

fn fetch_schema(
    tree: &TransactionalTree,
    table_name: &str,
) -> ConflictableTransactionResult<(String, Option<Snapshot<Schema>>), Error> {
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
