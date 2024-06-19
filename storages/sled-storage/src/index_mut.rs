use {
    super::{
        err_into,
        index_sync::IndexSync,
        key,
        lock::{self, LockAcquired},
        transaction::TxPayload,
        SledStorage, Snapshot,
    },
    async_io::block_on,
    async_trait::async_trait,
    futures::stream::TryStreamExt,
    gluesql_core::{
        ast::OrderByExpr,
        chrono::Utc,
        data::{Schema, SchemaIndex, SchemaIndexOrd},
        error::{Error, IndexError, Result},
        store::{IndexMut, Store},
    },
    sled::transaction::{
        ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
    },
    std::iter::once,
};

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

#[async_trait(?Send)]
impl IndexMut for SledStorage {
    async fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> Result<()> {
        let rows = self
            .scan_data(table_name)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_result = self.tree.transaction(move |tree| {
            let txid = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, .. } => txid,
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let index_expr = &column.expr;

            let (schema_key, schema_snapshot) = fetch_schema(tree, table_name)?;
            let schema_snapshot = schema_snapshot
                .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let (schema_snapshot, schema) = schema_snapshot.delete(txid);
            let Schema {
                column_defs,
                indexes,
                engine,
                foreign_keys,
                comment,
                ..
            } = schema
                .ok_or_else(|| IndexError::ConflictTableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            if indexes.iter().any(|index| index.name == index_name) {
                return Err(ConflictableTransactionError::Abort(
                    IndexError::IndexNameAlreadyExists(index_name.to_owned()).into(),
                ));
            }

            let index = SchemaIndex {
                name: index_name.to_owned(),
                expr: index_expr.clone(),
                order: SchemaIndexOrd::Both,
                created: Utc::now().naive_utc(),
            };

            let indexes = indexes
                .into_iter()
                .chain(once(index.clone()))
                .collect::<Vec<_>>();

            let schema = Schema {
                table_name: table_name.to_owned(),
                column_defs,
                indexes,
                engine,
                foreign_keys,
                comment,
            };

            let index_sync = IndexSync::from_schema(tree, txid, &schema);

            let schema_snapshot = schema_snapshot.update(txid, schema.clone());
            let schema_snapshot = bincode::serialize(&schema_snapshot)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            block_on(async {
                for (data_key, row) in rows.iter() {
                    let data_key = data_key
                        .to_cmp_be_bytes()
                        .map_err(ConflictableTransactionError::Abort)
                        .map(|key| key::data(table_name, key))?;

                    index_sync.insert_index(&index, &data_key, row).await?;
                }

                Ok(()) as ConflictableTransactionResult<(), Error>
            })?;

            tree.insert(schema_key.as_bytes(), schema_snapshot)?;

            let temp_key = key::temp_schema(txid, table_name);
            tree.insert(temp_key, schema_key.as_bytes())?;

            Ok(TxPayload::Success)
        });

        if self.check_retry(tx_result)? {
            self.create_index(table_name, index_name, column).await?;
        }

        Ok(())
    }

    async fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        let rows = self
            .scan_data(table_name)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_result = self.tree.transaction(move |tree| {
            let txid = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, .. } => txid,
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let (schema_key, schema_snapshot) = fetch_schema(tree, table_name)?;
            let schema_snapshot = schema_snapshot
                .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let (schema_snapshot, schema) = schema_snapshot.delete(txid);
            let Schema {
                column_defs,
                indexes,
                engine,
                foreign_keys,
                comment,
                ..
            } = schema
                .ok_or_else(|| IndexError::ConflictTableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let (index, indexes): (Vec<_>, _) = indexes
                .into_iter()
                .partition(|index| index.name == index_name);

            let index = match index.into_iter().next() {
                Some(index) => index,
                None => {
                    return Err(ConflictableTransactionError::Abort(
                        IndexError::IndexNameDoesNotExist(index_name.to_owned()).into(),
                    ));
                }
            };

            let schema = Schema {
                table_name: table_name.to_owned(),
                column_defs,
                indexes,
                engine,
                foreign_keys,
                comment,
            };

            let index_sync = IndexSync::from_schema(tree, txid, &schema);

            let schema_snapshot = schema_snapshot.update(txid, schema.clone());
            let schema_snapshot = bincode::serialize(&schema_snapshot)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            block_on(async {
                for (data_key, row) in rows.iter() {
                    let data_key = data_key
                        .to_cmp_be_bytes()
                        .map_err(ConflictableTransactionError::Abort)
                        .map(|key| key::data(table_name, key))?;

                    index_sync.delete_index(&index, &data_key, row).await?;
                }

                Ok(()) as ConflictableTransactionResult<(), Error>
            })?;

            tree.insert(schema_key.as_bytes(), schema_snapshot)?;

            let temp_key = key::temp_schema(txid, table_name);
            tree.insert(temp_key, schema_key.as_bytes())?;

            Ok(TxPayload::Success)
        });

        if self.check_retry(tx_result)? {
            self.drop_index(table_name, index_name).await?;
        }

        Ok(())
    }
}
