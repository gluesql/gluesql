use {
    super::{
        err_into,
        index_sync::IndexSync,
        key,
        lock::{self, LockAcquired},
        transaction::TxPayload,
        tx_err_into, SledStorage, Snapshot,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        result::Result,
        store::{DataRow, IndexError, StoreMut},
    },
    sled::transaction::ConflictableTransactionError,
};

#[async_trait]
impl StoreMut for SledStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
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

        if let TxPayload::RollbackAndRetry(lock_txid) = tx_result.map_err(tx_err_into)? {
            self.rollback_txid(lock_txid)?;
            self.tree
                .transaction(move |tree| lock::release(tree, lock_txid))
                .map_err(tx_err_into)?;

            self.insert_schema(schema).await?;
        }

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let prefix = format!("data/{}/", table_name);
        let items = self
            .tree
            .scan_prefix(prefix.as_bytes())
            .map(|item| item.map_err(err_into))
            .collect::<Result<Vec<_>>>()?;

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
                let row_snapshot: Snapshot<DataRow> = bincode::deserialize(row_snapshot)
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

        if let TxPayload::RollbackAndRetry(lock_txid) = tx_result.map_err(tx_err_into)? {
            self.rollback_txid(lock_txid)?;
            self.tree
                .transaction(move |tree| lock::release(tree, lock_txid))
                .map_err(tx_err_into)?;

            self.delete_schema(table_name).await?;
        }

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        let id_offset = self.id_offset;
        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_rows = &rows;

        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let index_sync = IndexSync::new(tree, txid, table_name)?;

            for row in tx_rows.iter() {
                let id = id_offset + tree.generate_id()?;
                let id = id.to_be_bytes();
                let key = key::data(table_name, id.to_vec());

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

        if let TxPayload::RollbackAndRetry(lock_txid) = tx_result.map_err(tx_err_into)? {
            self.rollback_txid(lock_txid)?;
            self.tree
                .transaction(move |tree| lock::release(tree, lock_txid))
                .map_err(tx_err_into)?;

            self.append_data(table_name, rows).await?;
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_rows = &rows;

        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let index_sync = IndexSync::new(tree, txid, table_name)?;

            for (key, new_row) in tx_rows.iter() {
                let key = key::data(table_name, key.to_cmp_be_bytes());
                let snapshot = match tree.get(&key)? {
                    Some(snapshot) => {
                        let snapshot: Snapshot<DataRow> = bincode::deserialize(&snapshot)
                            .map_err(err_into)
                            .map_err(ConflictableTransactionError::Abort)?;

                        let (snapshot, old_row) = snapshot.update(txid, new_row.clone());
                        let old_row = match old_row {
                            Some(row) => row,
                            None => {
                                continue;
                            }
                        };

                        index_sync.update(&key, &old_row, new_row)?;

                        snapshot
                    }
                    None => {
                        index_sync.insert(&key, new_row)?;

                        Snapshot::new(txid, new_row.clone())
                    }
                };

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

        if let TxPayload::RollbackAndRetry(lock_txid) = tx_result.map_err(tx_err_into)? {
            self.rollback_txid(lock_txid)?;
            self.tree
                .transaction(move |tree| lock::release(tree, lock_txid))
                .map_err(tx_err_into)?;

            self.insert_data(table_name, rows).await?;
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_keys = &keys;

        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let index_sync = IndexSync::new(tree, txid, table_name)?;

            for key in tx_keys.iter() {
                let key = key::data(table_name, key.to_cmp_be_bytes());
                let snapshot = tree
                    .get(&key)?
                    .ok_or_else(|| IndexError::ConflictOnEmptyIndexValueDelete.into())
                    .map_err(ConflictableTransactionError::Abort)?;
                let snapshot: Snapshot<DataRow> = bincode::deserialize(&snapshot)
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
                    .map(|snapshot| tree.insert(&key, snapshot))??;

                index_sync.delete(&key, &row)?;

                if !autocommit {
                    let temp_key = key::temp_data(txid, &key);

                    tree.insert(temp_key, key)?;
                }
            }

            Ok(TxPayload::Success)
        });

        if let TxPayload::RollbackAndRetry(lock_txid) = tx_result.map_err(tx_err_into)? {
            self.rollback_txid(lock_txid)?;
            self.tree
                .transaction(move |tree| lock::release(tree, lock_txid))
                .map_err(tx_err_into)?;

            self.delete_data(table_name, keys).await?;
        }

        Ok(())
    }
}
