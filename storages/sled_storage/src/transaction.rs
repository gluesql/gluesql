use {
    super::{
        err_into,
        error::StorageError,
        key,
        lock::{self, Lock},
        tx_err_into, SledStorage, Snapshot, State,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::{Row, Schema},
        result::MutResult,
        result::{Error, Result},
        store::Transaction,
    },
    serde::{de::DeserializeOwned, Serialize},
    sled::{
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionError,
            TransactionalTree,
        },
        IVec,
    },
    std::{fmt::Debug, result::Result as StdResult},
};

macro_rules! transaction {
    ($self: expr, $expr: expr) => {{
        let result = $self.tree.transaction($expr).map_err(|e| match e {
            TransactionError::Abort(e) => e,
            TransactionError::Storage(e) => StorageError::Sled(e).into(),
        });

        match result {
            Ok(v) => {
                let storage = Self {
                    tree: $self.tree,
                    state: State::Idle,
                    tx_timeout: $self.tx_timeout,
                };

                Ok((storage, v))
            }
            Err(e) => Err(($self, e)),
        }
    }};
}

pub enum TxPayload {
    Success,
    RollbackAndRetry(u64),
}

#[async_trait(?Send)]
impl Transaction for SledStorage {
    async fn begin(self, autocommit: bool) -> MutResult<Self, bool> {
        match (&self.state, autocommit) {
            (State::Transaction { .. }, false) => Err((
                self,
                Error::StorageMsg("nested transaction is not supported".to_owned()),
            )),
            (State::Transaction { autocommit, .. }, true) => {
                let autocommit = *autocommit;

                Ok((self, autocommit))
            }
            (State::Idle, _) => match lock::register(&self.tree) {
                Ok((txid, created_at)) => {
                    let state = State::Transaction {
                        txid,
                        created_at,
                        autocommit,
                    };

                    Ok((self.update_state(state), autocommit))
                }
                Err(e) => Err((self, e)),
            },
        }
    }

    async fn rollback(self) -> MutResult<Self, ()> {
        let txid = match self.state {
            State::Transaction { txid, .. } => txid,
            State::Idle => {
                return Err((
                    self,
                    Error::StorageMsg("no transaction to rollback".to_owned()),
                ));
            }
        };

        let rollback = || {
            let Lock { lock_txid, .. } = self
                .tree
                .get("lock/")
                .map_err(err_into)?
                .map(|l| bincode::deserialize(&l))
                .transpose()
                .map_err(err_into)?
                .unwrap_or_default();

            if Some(txid) == lock_txid {
                self.rollback_txid(txid).map(|_| lock_txid)
            } else {
                Ok(None)
            }
        };

        match rollback() {
            Ok(lock_txid) => transaction!(self, move |tree| {
                lock_txid
                    .map(|lock_txid| lock::release(tree, lock_txid))
                    .transpose()
            })
            .map(|(storage, _)| (storage.update_state(State::Idle), ())),
            Err(e) => Err((self, e)),
        }
    }

    async fn commit(self) -> MutResult<Self, ()> {
        let (txid, created_at) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at),
            State::Idle => {
                return Err((
                    self,
                    Error::StorageMsg("no transaction to commit".to_owned()),
                ));
            }
        };

        if let Err(e) = lock::fetch(&self.tree, txid, created_at, self.tx_timeout) {
            return Err((self, e));
        }

        let (storage, _) = transaction!(self, move |tree| { lock::release(tree, txid) })?;
        let gc = || {
            if storage.tree.get("gc_lock").map_err(err_into)?.is_some() {
                return Ok(());
            }

            storage.tree.insert("gc_lock", &[1]).map_err(err_into)?;

            let gc_result = storage.gc();

            storage.tree.remove("gc_lock").map_err(err_into)?;

            gc_result
        };

        match gc() {
            Ok(_) => Ok((storage, ())),
            Err(e) => Err((storage, e)),
        }
    }
}

impl SledStorage {
    pub fn rollback_txid(&self, txid: u64) -> Result<()> {
        let fetch_items = |prefix| {
            self.tree
                .scan_prefix(prefix)
                .map(|item| item.map_err(err_into))
                .collect::<Result<Vec<_>>>()
        };

        fn rollback_items<T: Debug + Clone + Serialize + DeserializeOwned>(
            tree: &TransactionalTree,
            txid: u64,
            items: &[(IVec, IVec)],
        ) -> ConflictableTransactionResult<(), Error> {
            for (temp_key, value_key) in items.iter() {
                tree.remove(temp_key)?;

                let snapshot = tree
                    .get(value_key)?
                    .map(|l| bincode::deserialize(&l))
                    .transpose()
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let snapshot: Snapshot<T> = match snapshot {
                    Some(snapshot) => snapshot,
                    None => {
                        continue;
                    }
                };

                match snapshot.rollback(txid) {
                    Some(snapshot) => {
                        let snapshot = bincode::serialize(&snapshot)
                            .map_err(err_into)
                            .map_err(ConflictableTransactionError::Abort)?;

                        tree.insert(value_key, snapshot)?;
                    }
                    None => {
                        tree.remove(value_key)?;
                    }
                };
            }

            Ok(())
        }

        let data_items = fetch_items(key::temp_data_prefix(txid))?;
        let schema_items = fetch_items(key::temp_schema_prefix(txid))?;
        let index_items = fetch_items(key::temp_index_prefix(txid))?;

        self.tree
            .transaction(move |tree| {
                rollback_items::<Row>(tree, txid, &data_items)?;
                rollback_items::<Schema>(tree, txid, &schema_items)?;

                for (temp_key, value_key) in index_items.iter() {
                    tree.remove(temp_key)?;

                    let snapshots = tree
                        .get(value_key)?
                        .map(|l| bincode::deserialize(&l))
                        .transpose()
                        .map_err(err_into)
                        .map_err(ConflictableTransactionError::Abort)?;

                    let snapshots: Vec<Snapshot<Vec<u8>>> = match snapshots {
                        Some(snapshots) => snapshots,
                        None => {
                            continue;
                        }
                    };

                    let snapshots = snapshots
                        .into_iter()
                        .filter_map(|snapshot| snapshot.rollback(txid))
                        .collect::<Vec<_>>();

                    if snapshots.is_empty() {
                        tree.remove(value_key)?;
                    } else {
                        let snapshots = bincode::serialize(&snapshots)
                            .map_err(err_into)
                            .map_err(ConflictableTransactionError::Abort)?;

                        tree.insert(value_key, snapshots)?;
                    }
                }

                Ok(())
            })
            .map_err(tx_err_into)
    }

    pub async fn check_and_retry<Fut>(
        self,
        tx_result: StdResult<TxPayload, TransactionError<Error>>,
        retry_func: impl FnOnce(SledStorage) -> Fut,
    ) -> MutResult<SledStorage, ()>
    where
        Fut: futures::Future<Output = MutResult<SledStorage, ()>>,
    {
        match tx_result.map_err(tx_err_into) {
            Ok(TxPayload::Success) => Ok((self, ())),
            Ok(TxPayload::RollbackAndRetry(lock_txid)) => {
                if let Err(err) = self.rollback_txid(lock_txid) {
                    return Err((self, err));
                };

                match self
                    .tree
                    .transaction(move |tree| lock::release(tree, lock_txid))
                    .map_err(tx_err_into)
                {
                    Ok(_) => retry_func(self).await,
                    Err(err) => Err((self, err)),
                }
            }
            Err(err) => Err((self, err)),
        }
    }
}
