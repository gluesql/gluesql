use {
    super::{err_into, State},
    crate::{Error, Result},
    serde::{Deserialize, Serialize},
    sled::{
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
        },
        Db,
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct TxData {
    pub txid: u64,
    pub alive: bool,
    // TODO: support timeout based garbage collection
    // - created_at: u128,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Lock {
    pub lock_txid: Option<u64>,
    pub gc_txid: Option<u64>,
    // TODO: support serializable transaction isolation level
    // - prev_done_at: u128,
}

pub fn get_txdata_key(txid: u64) -> Vec<u8> {
    "tx_data/"
        .to_owned()
        .into_bytes()
        .into_iter()
        .chain(txid.to_be_bytes().iter().copied())
        .collect::<Vec<_>>()
}

pub fn register(tree: &Db) -> Result<u64> {
    let txid = tree.generate_id().map_err(err_into)?;
    let key = get_txdata_key(txid);
    let tx_data = TxData { txid, alive: true };

    bincode::serialize(&tx_data)
        .map_err(err_into)
        .map(|tx_data| tree.insert(key, tx_data))?
        .map_err(err_into)?;

    Ok(txid)
}

pub fn fetch(tree: &Db, txid: u64) -> Result<Option<u64>> {
    let Lock { lock_txid, gc_txid } = tree
        .get("lock/")
        .map_err(err_into)?
        .map(|l| bincode::deserialize(&l))
        .transpose()
        .map_err(err_into)?
        .unwrap_or_default();

    if gc_txid.is_some() && Some(txid) <= gc_txid {
        return Err(Error::StorageMsg(
            "fetch failed - expired transaction is used".to_owned(),
        ));
    }

    Ok(lock_txid)
}

pub fn acquire(
    tree: &TransactionalTree,
    state: &State,
) -> ConflictableTransactionResult<(u64, bool), Error> {
    let Lock { lock_txid, gc_txid } = tree
        .get("lock/")?
        .map(|l| bincode::deserialize(&l))
        .transpose()
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?
        .unwrap_or_default();

    let (txid, autocommit) = match state {
        State::Transaction { txid, autocommit } => (txid, autocommit),
        State::Idle => {
            return Err(Error::StorageMsg(
                "conflict - cannot acquire lock from idle state".to_owned(),
            ))
            .map_err(ConflictableTransactionError::Abort);
        }
    };

    if gc_txid.is_some() && Some(txid) <= gc_txid.as_ref() {
        return Err(Error::StorageMsg(
            "acquire failed - expired transaction is used".to_owned(),
        ))
        .map_err(ConflictableTransactionError::Abort);
    }

    let txid = match lock_txid {
        Some(lock_txid) if txid == &lock_txid => txid,
        Some(_) => {
            return Err(Error::StorageMsg("database is locked".to_owned()))
                .map_err(ConflictableTransactionError::Abort);
        }
        None => {
            let lock = Lock {
                lock_txid: Some(*txid),
                gc_txid,
            };

            bincode::serialize(&lock)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)
                .map(|lock| tree.insert("lock/", lock))??;

            txid
        }
    };

    Ok((*txid, *autocommit))
}

pub fn unregister(tree: &Db, txid: u64) -> Result<()> {
    let key = get_txdata_key(txid);
    let mut tx_data: TxData = tree
        .get(&key)
        .map_err(err_into)?
        .ok_or_else(|| Error::StorageMsg("conflict - lock does not exist".to_owned()))
        .map(|tx_data| bincode::deserialize(&tx_data))?
        .map_err(err_into)?;

    tx_data.alive = false;

    bincode::serialize(&tx_data)
        .map(|tx_data| tree.insert(key, tx_data))
        .map_err(err_into)?
        .map_err(err_into)?;

    Ok(())
}

pub fn release(tree: &TransactionalTree, txid: u64) -> ConflictableTransactionResult<(), Error> {
    let Lock { gc_txid, .. } = tree
        .get("lock/")?
        .map(|l| bincode::deserialize(&l))
        .transpose()
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?
        .unwrap_or_default();

    let lock = Lock {
        lock_txid: None,
        gc_txid,
    };

    bincode::serialize(&lock)
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)
        .map(|lock| tree.insert("lock/", lock))??;

    let key = get_txdata_key(txid);
    let mut tx_data: TxData = tree
        .get(&key)?
        .ok_or_else(|| Error::StorageMsg("conflict - lock does not exist".to_owned()))
        .map(|tx_data| bincode::deserialize(&tx_data))
        .map_err(ConflictableTransactionError::Abort)?
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?;

    tx_data.alive = false;

    bincode::serialize(&tx_data)
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)
        .map(|tx_data| tree.insert(key, tx_data))??;

    Ok(())
}
