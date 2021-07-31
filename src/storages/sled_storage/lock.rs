use {
    super::{err_into, State},
    crate::{Error, Result},
    sled::{
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
        },
        Db,
    },
};

pub fn fetch(tree: &Db) -> Result<Option<u64>> {
    tree.get("lock/")
        .map_err(err_into)?
        .map(|l| bincode::deserialize(&l))
        .transpose()
        .map_err(err_into)
}

pub fn acquire(
    tree: &TransactionalTree,
    state: &State,
) -> ConflictableTransactionResult<(u64, bool), Error> {
    let lock_txid: Option<u64> = tree
        .get("lock/")?
        .map(|l| bincode::deserialize(&l))
        .transpose()
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?;

    let (txid, autocommit) = match state {
        State::Transaction { txid, autocommit } => (txid, autocommit),
        State::Idle => {
            return Err(Error::StorageMsg(
                "conflict - cannot acquire lock from idle state".to_owned(),
            ))
            .map_err(ConflictableTransactionError::Abort);
        }
    };

    let txid = match lock_txid {
        Some(lock_txid) if txid == &lock_txid => txid,
        Some(_) => {
            return Err(Error::StorageMsg("database is locked".to_owned()))
                .map_err(ConflictableTransactionError::Abort);
        }
        None => {
            let lock = bincode::serialize(txid)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            tree.insert("lock/", lock)?;

            txid
        }
    };

    Ok((*txid, *autocommit))
}

pub fn release(tree: &TransactionalTree) -> ConflictableTransactionResult<(), Error> {
    tree.remove("lock/")?;

    Ok(())
}
