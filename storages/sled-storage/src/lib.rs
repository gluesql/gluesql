#![deny(clippy::str_to_string)]

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

// re-export
pub use sled;

use {
    self::snapshot::Snapshot,
    error::{err_into, tx_err_into},
    gluesql_core::{
        data::Schema,
        error::{Error, Result},
        store::Metadata,
    },
    sled::{
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
        },
        Config, Db,
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
    pub id_offset: u64,
    pub state: State,
    /// transaction timeout in milliseconds
    pub tx_timeout: Option<u128>,
}

type ExportData<T> = (u64, Vec<(Vec<u8>, Vec<u8>, T)>);

impl SledStorage {
    pub fn new<P: AsRef<std::path::Path>>(filename: P) -> Result<Self> {
        let tree = sled::open(filename).map_err(err_into)?;
        let id_offset = get_id_offset(&tree)?;
        let state = State::Idle;
        let tx_timeout = Some(DEFAULT_TX_TIMEOUT);

        Ok(Self {
            tree,
            id_offset,
            state,
            tx_timeout,
        })
    }

    pub fn set_transaction_timeout(&mut self, tx_timeout: Option<u128>) {
        self.tx_timeout = tx_timeout;
    }

    pub fn export(&self) -> Result<ExportData<impl Iterator<Item = Vec<Vec<u8>>>>> {
        let id_offset = self.id_offset + self.tree.generate_id().map_err(err_into)?;
        let data = self.tree.export();

        Ok((id_offset, data))
    }

    pub fn import(&mut self, export: ExportData<impl Iterator<Item = Vec<Vec<u8>>>>) -> Result<()> {
        let (new_id_offset, data) = export;
        let old_id_offset = get_id_offset(&self.tree)?;

        self.tree.import(data);

        if new_id_offset > old_id_offset {
            self.tree
                .insert("id_offset", &new_id_offset.to_be_bytes())
                .map_err(err_into)?;

            self.id_offset = new_id_offset;
        }

        Ok(())
    }
}

impl TryFrom<Config> for SledStorage {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        let tree = config.open().map_err(err_into)?;
        let id_offset = get_id_offset(&tree)?;
        let state = State::Idle;
        let tx_timeout = Some(DEFAULT_TX_TIMEOUT);

        Ok(Self {
            tree,
            id_offset,
            state,
            tx_timeout,
        })
    }
}

fn get_id_offset(tree: &Db) -> Result<u64> {
    tree.get("id_offset")
        .map_err(err_into)?
        .map(|id| {
            id.as_ref()
                .try_into()
                .map_err(err_into)
                .map(u64::from_be_bytes)
        })
        .unwrap_or(Ok(0))
}

fn fetch_schema(
    tree: &TransactionalTree,
    table_name: &str,
) -> ConflictableTransactionResult<(String, Option<Snapshot<Schema>>), Error> {
    let key = format!("schema/{}", table_name);
    let value = tree.get(key.as_bytes())?;
    let schema_snapshot = value
        .map(|v| bincode::deserialize(&v))
        .transpose()
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?;

    Ok((key, schema_snapshot))
}

impl Metadata for SledStorage {}
impl gluesql_core::store::CustomFunction for SledStorage {}
impl gluesql_core::store::CustomFunctionMut for SledStorage {}
