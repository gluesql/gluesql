use {
    super::{
        err_into, key,
        lock::{self, Lock},
        tx_err_into, SledStorage, Snapshot, State,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::Schema,
        result::{Error, Result},
        store::{DataRow, Transaction},
    },
    serde::{de::DeserializeOwned, Serialize},
    sled::{
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionError,
            TransactionalTree,
        },
        IVec,
    },
    std::result::Result as StdResult,
};

pub enum TxPayload {
    Success,
    RollbackAndRetry(u64),
}

#[async_trait]
impl Transaction for SledStorage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        match (&self.state, autocommit) {
            (State::Transaction { .. }, false) => Err(Error::StorageMsg(
                "nested transaction is not supported".to_owned(),
            )),
            (State::Transaction { autocommit, .. }, true) => Ok(*autocommit),
            (State::Idle, _) => {
                let (txid, created_at) = lock::register(&self.tree, self.id_offset)?;

                self.state = State::Transaction {
                    txid,
                    created_at,
                    autocommit,
                };

                Ok(autocommit)
            }
        }
    }

    async fn rollback(&mut self) -> Result<()> {
        let txid = match self.state {
            State::Transaction { txid, .. } => txid,
            State::Idle => {
                return Err(Error::StorageMsg("no transaction to rollback".to_owned()));
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

        let lock_txid = rollback()?;

        self.tree
            .transaction(move |tree| {
                lock_txid
                    .map(|lock_txid| lock::release(tree, lock_txid))
                    .transpose()
            })
            .map_err(tx_err_into)?;

        self.state = State::Idle;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        let (txid, created_at) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at),
            State::Idle => {
                return Err(Error::StorageMsg("no transaction to commit".to_owned()));
            }
        };

        lock::fetch(&self.tree, txid, created_at, self.tx_timeout)?;

        self.tree
            .transaction(move |tree| lock::release(tree, txid))
            .map_err(tx_err_into)?;

        self.state = State::Idle;

        if self.tree.get("gc_lock").map_err(err_into)?.is_some() {
            return Ok(());
        }

        self.tree.insert("gc_lock", &[1]).map_err(err_into)?;

        let gc_result = self.gc();

        self.tree.remove("gc_lock").map_err(err_into)?;

        gc_result
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

        fn rollback_items<T: Clone + Serialize + DeserializeOwned>(
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
                rollback_items::<DataRow>(tree, txid, &data_items)?;
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

    pub fn check_retry(
        &mut self,
        tx_result: StdResult<TxPayload, TransactionError<Error>>,
    ) -> Result<bool> {
        if let TxPayload::RollbackAndRetry(lock_txid) = tx_result.map_err(tx_err_into)? {
            self.rollback_txid(lock_txid)?;
            self.tree
                .transaction(move |tree| lock::release(tree, lock_txid))
                .map_err(tx_err_into)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }
}
