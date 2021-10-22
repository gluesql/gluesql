use {
    super::{
        err_into,
        index_sync::IndexSync,
        key,
        lock::{self, LockAcquired},
        transaction::TxPayload,
        SledStorage, Snapshot,
    },
    crate::{
        data::{Row, Schema},
        result::Result,
        IndexError, MutResult, StoreMut,
    },
    async_trait::async_trait,
    sled::{transaction::ConflictableTransactionError, IVec},
    std::rc::Rc,
};

#[async_trait(?Send)]
impl StoreMut<IVec> for SledStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let state = &self.state;
        let tx_timeout = self.tx_timeout;

        let tx_result = self.tree.transaction(move |tree| {
            let txid = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, .. } => txid,
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let key = format!("schema/{}", schema.table_name);
            let temp_key = key::temp_schema(txid, &schema.table_name);

            let snapshot: Option<Snapshot<Schema>> = tree
                .get(key.as_bytes())?
                .map(|v| bincode::deserialize(&v))
                .transpose()
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            let schema = schema.clone();
            let snapshot = match snapshot {
                Some(snapshot) => snapshot.update(txid, schema).0,
                None => Snapshot::<Schema>::new(txid, schema),
            };
            let snapshot = bincode::serialize(&snapshot)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            tree.insert(key.as_bytes(), snapshot)?;
            tree.insert(temp_key, key.as_bytes())?;

            Ok(TxPayload::Success)
        });

        self.retry(tx_result, |storage| storage.insert_schema(schema))
            .await
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let prefix = format!("data/{}/", table_name);
        let items = self
            .tree
            .scan_prefix(prefix.as_bytes())
            .map(|item| item.map_err(err_into))
            .collect::<Result<Vec<_>>>();
        let items = match items {
            Ok(items) => items,
            Err(e) => {
                return Err((self, e));
            }
        };

        let state = &self.state;
        let tx_timeout = self.tx_timeout;

        let tx_result = self.tree.transaction(move |tree| {
            let txid = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, .. } => txid,
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let key = format!("schema/{}", table_name);
            let temp_key = key::temp_schema(txid, table_name);

            let snapshot: Option<Snapshot<Schema>> = tree
                .get(key.as_bytes())?
                .map(|v| bincode::deserialize(&v))
                .transpose()
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            let (snapshot, schema) = match snapshot.map(|snapshot| snapshot.delete(txid)) {
                Some((snapshot, Some(schema))) => (snapshot, schema),
                Some((_, None)) | None => {
                    return Ok(TxPayload::Success);
                }
            };
            let snapshot = bincode::serialize(&snapshot)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            tree.insert(key.as_bytes(), snapshot)?;
            tree.insert(temp_key, key.as_bytes())?;

            let index_sync = IndexSync::from_schema(tree, txid, &schema);

            // delete data
            for (row_key, row_snapshot) in items.iter() {
                let row_snapshot: Snapshot<Row> = bincode::deserialize(row_snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let (row_snapshot, deleted_row) = row_snapshot.delete(txid);
                let deleted_row = match deleted_row {
                    Some(row) => row,
                    None => {
                        continue;
                    }
                };

                let row_snapshot = bincode::serialize(&row_snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let temp_row_key = key::temp_data(txid, row_key);

                tree.insert(row_key, row_snapshot)?;
                tree.insert(temp_row_key, row_key)?;

                index_sync.delete(row_key, &deleted_row)?;
            }

            Ok(TxPayload::Success)
        });

        self.retry(tx_result, |storage| storage.delete_schema(table_name))
            .await
    }

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        self.insert_data_sync(table_name, Rc::new(rows))
    }

    async fn update_data(self, table_name: &str, rows: Vec<(IVec, Row)>) -> MutResult<Self, ()> {
        self.update_data_sync(table_name, Rc::new(rows))
    }

    async fn delete_data(self, table_name: &str, keys: Vec<IVec>) -> MutResult<Self, ()> {
        self.delete_data_sync(table_name, Rc::new(keys))
    }
}

impl SledStorage {
    fn insert_data_sync(self, table_name: &str, rows: Rc<Vec<Row>>) -> MutResult<Self, ()> {
        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_rows = Rc::clone(&rows);
        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let index_sync = IndexSync::new(tree, txid, table_name)?;

            for row in tx_rows.iter() {
                let id = tree.generate_id()?;
                let id = id.to_be_bytes();
                let prefix = format!("data/{}/", table_name);

                let bytes = prefix
                    .into_bytes()
                    .into_iter()
                    .chain(id.iter().copied())
                    .collect::<Vec<_>>();

                let key = IVec::from(bytes);

                index_sync.insert(&key, row)?;

                let snapshot = Snapshot::new(txid, row.clone());
                let snapshot = bincode::serialize(&snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                tree.insert(&key, snapshot)?;

                if !autocommit {
                    let temp_key = key::temp_data(txid, &key);

                    tree.insert(temp_key, key)?;
                }
            }

            Ok(TxPayload::Success)
        });

        self.retry_sync(tx_result, |storage| {
            storage.insert_data_sync(table_name, rows)
        })
    }

    fn update_data_sync(self, table_name: &str, rows: Rc<Vec<(IVec, Row)>>) -> MutResult<Self, ()> {
        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_rows = Rc::clone(&rows);
        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let index_sync = IndexSync::new(tree, txid, table_name)?;

            for (key, new_row) in tx_rows.iter() {
                let snapshot = tree
                    .get(key)?
                    .ok_or_else(|| IndexError::ConflictOnEmptyIndexValueDelete.into())
                    .map_err(ConflictableTransactionError::Abort)?;
                let snapshot: Snapshot<Row> = bincode::deserialize(&snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let (snapshot, old_row) = snapshot.update(txid, new_row.clone());
                let old_row = match old_row {
                    Some(row) => row,
                    None => {
                        continue;
                    }
                };

                bincode::serialize(&snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)
                    .map(|snapshot| tree.insert(key, snapshot))??;

                index_sync.update(key, &old_row, new_row)?;

                if !autocommit {
                    let temp_key = key::temp_data(txid, key);

                    tree.insert(temp_key, key)?;
                }
            }

            Ok(TxPayload::Success)
        });

        self.retry_sync(tx_result, |storage| {
            storage.update_data_sync(table_name, rows)
        })
    }

    fn delete_data_sync(self, table_name: &str, keys: Rc<Vec<IVec>>) -> MutResult<Self, ()> {
        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_keys = Rc::clone(&keys);
        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let index_sync = IndexSync::new(tree, txid, table_name)?;

            for key in tx_keys.iter() {
                let snapshot = tree
                    .get(key)?
                    .ok_or_else(|| IndexError::ConflictOnEmptyIndexValueDelete.into())
                    .map_err(ConflictableTransactionError::Abort)?;
                let snapshot: Snapshot<Row> = bincode::deserialize(&snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let (snapshot, row) = snapshot.delete(txid);
                let row = match row {
                    Some(row) => row,
                    None => {
                        continue;
                    }
                };

                bincode::serialize(&snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)
                    .map(|snapshot| tree.insert(key, snapshot))??;

                index_sync.delete(key, &row)?;

                if !autocommit {
                    let temp_key = key::temp_data(txid, key);

                    tree.insert(temp_key, key)?;
                }
            }

            Ok(TxPayload::Success)
        });

        self.retry_sync(tx_result, |storage| {
            storage.delete_data_sync(table_name, keys)
        })
    }
}
