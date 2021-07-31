#![cfg(feature = "transaction")]

use {
    super::{
        err_into,
        error::StorageError,
        key::{TEMP_DATA, TEMP_INDEX, TEMP_SCHEMA},
        lock, SledStorage, Snapshot, State,
    },
    crate::{
        data::{Row, Schema},
        Error, MutResult, Result, Transaction,
    },
    async_trait::async_trait,
    sled::transaction::{ConflictableTransactionError, TransactionError},
};

macro_rules! try_block {
    ($self: expr, $block: block) => {{
        let block = || $block;

        match block() {
            Err(e) => {
                return Err(($self, e));
            }
            Ok(v) => v,
        }
    }};
}

macro_rules! transaction {
    ($self: expr, $expr: expr) => {{
        let result = $self.tree.transaction($expr).map_err(|e| match e {
            TransactionError::Abort(e) => e,
            TransactionError::Storage(e) => StorageError::Sled(e).into(),
        });

        match result {
            Ok(_) => {
                let storage = Self {
                    tree: $self.tree,
                    state: State::Idle,
                };

                Ok((storage, ()))
            }
            Err(e) => Err(($self, e)),
        }
    }};
}

#[async_trait(?Send)]
impl Transaction for SledStorage {
    async fn begin(self, autocommit: bool) -> MutResult<Self, bool> {
        let (txid, autocommit) = try_block!(self, {
            match (&self.state, autocommit) {
                (State::Transaction { .. }, false) => Err(Error::StorageMsg(
                    "nested transaction is not supported".to_owned(),
                )),
                (State::Transaction { txid, autocommit }, true) => Ok((*txid, *autocommit)),
                (State::Idle, _) => self
                    .tree
                    .generate_id()
                    .map_err(err_into)
                    .map(|txid| (txid, autocommit)),
            }
        });

        let storage = Self {
            tree: self.tree,
            state: State::Transaction { txid, autocommit },
        };

        Ok((storage, autocommit))
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

        let fetch_items = |prefix| {
            self.tree
                .scan_prefix(prefix)
                .map(|item| item.map_err(err_into))
                .collect::<Result<Vec<_>>>()
        };

        let temp_items = try_block!(self, {
            let lock_txid = lock::fetch(&self.tree)?;

            if Some(txid) != lock_txid {
                return Ok(None);
            }

            let data_items = fetch_items(TEMP_DATA)?;
            let schema_items = fetch_items(TEMP_SCHEMA)?;
            let index_items = fetch_items(TEMP_INDEX)?;

            Ok(Some((data_items, schema_items, index_items)))
        });

        let (data_items, schema_items, index_items) = match temp_items {
            Some(items) => items,
            None => {
                return Ok((
                    Self {
                        tree: self.tree,
                        state: State::Idle,
                    },
                    (),
                ));
            }
        };

        transaction!(self, move |tree| {
            for (temp_key, value_key) in data_items.iter() {
                tree.remove(temp_key)?;

                let snapshot = tree
                    .get(value_key)?
                    .map(|l| bincode::deserialize(&l))
                    .transpose()
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let snapshot: Snapshot<Row> = match snapshot {
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

            for (temp_key, value_key) in schema_items.iter() {
                tree.remove(temp_key)?;

                let snapshot = tree
                    .get(value_key)?
                    .map(|l| bincode::deserialize(&l))
                    .transpose()
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let snapshot: Snapshot<Schema> = match snapshot {
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

            lock::release(tree)?;

            Ok(())
        })
    }

    async fn commit(self) -> MutResult<Self, ()> {
        let txid = match self.state {
            State::Transaction { txid, .. } => txid,
            State::Idle => {
                return Err((
                    self,
                    Error::StorageMsg("no transaction to commit".to_owned()),
                ));
            }
        };

        let fetch_keys = |prefix| {
            self.tree
                .scan_prefix(prefix)
                .map(|item| item.map(|(key, _)| key).map_err(err_into))
                .collect::<Result<Vec<_>>>()
        };

        let temp_keys = try_block!(self, {
            let lock_txid = lock::fetch(&self.tree)?;

            if Some(txid) != lock_txid {
                return Ok(None);
            }

            let data_keys = fetch_keys(TEMP_DATA)?;
            let schema_keys = fetch_keys(TEMP_SCHEMA)?;
            let index_keys = fetch_keys(TEMP_INDEX)?;

            Ok(Some((data_keys, schema_keys, index_keys)))
        });

        let (data_keys, schema_keys, index_keys) = match temp_keys {
            Some(keys) => keys,
            None => {
                return Ok((
                    Self {
                        tree: self.tree,
                        state: State::Idle,
                    },
                    (),
                ));
            }
        };

        transaction!(self, move |tree| {
            for key in data_keys.iter() {
                tree.remove(key)?;
            }

            for key in schema_keys.iter() {
                tree.remove(key)?;
            }

            for key in index_keys.iter() {
                tree.remove(key)?;
            }

            lock::release(tree)?;

            Ok(())
        })
    }
}
