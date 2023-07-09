use {
    super::{
        error::err_into,
        fetch_schema, key,
        lock::{self, LockAcquired},
        transaction::TxPayload,
        SledStorage, Snapshot,
    },
    async_io::block_on,
    async_trait::async_trait,
    gluesql_core::{
        ast::ColumnDef,
        data::{schema::Schema, Value},
        error::{AlterTableError, Error, Result},
        executor::evaluate_stateless,
        store::{AlterTable, DataRow},
    },
    sled::transaction::ConflictableTransactionError,
    std::{iter::once, str},
    utils::Vector,
};

#[async_trait(?Send)]
impl AlterTable for SledStorage {
    async fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        let prefix = format!("data/{}/", table_name);
        let items = self
            .tree
            .scan_prefix(prefix.as_bytes())
            .map(|item| item.map_err(err_into))
            .collect::<Result<Vec<_>>>()?;

        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let (old_schema_key, schema_snapshot) = fetch_schema(tree, table_name)?;
            let schema_snapshot = schema_snapshot
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            // remove existing schema
            let (old_snapshot, old_schema) = schema_snapshot.delete(txid);
            let Schema {
                column_defs,
                indexes,
                engine,
                ..
            } = old_schema
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let new_schema = Schema {
                table_name: new_table_name.to_owned(),
                column_defs,
                indexes,
                engine,
            };

            bincode::serialize(&old_snapshot)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)
                .map(|snapshot| tree.insert(old_schema_key.as_bytes(), snapshot))??;

            // insert new schema
            let new_snapshot = Snapshot::<Schema>::new(txid, new_schema);
            let value = bincode::serialize(&new_snapshot)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;
            let new_schema_key = format!("schema/{}", new_table_name);
            tree.insert(new_schema_key.as_bytes(), value)?;

            // replace data
            for (old_key, value) in items.iter() {
                let new_key = str::from_utf8(old_key.as_ref())
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;
                let new_key = new_key.replace(table_name, new_table_name);

                let old_row_snapshot: Snapshot<DataRow> = bincode::deserialize(value)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let (old_row_snapshot, row) = old_row_snapshot.delete(txid);
                let row = match row {
                    Some(row) => row,
                    None => {
                        continue;
                    }
                };

                let old_row_snapshot = bincode::serialize(&old_row_snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let new_row_snapshot = Snapshot::<DataRow>::new(txid, row);
                let new_row_snapshot = bincode::serialize(&new_row_snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                tree.insert(old_key, old_row_snapshot)?;
                tree.insert(new_key.as_bytes(), new_row_snapshot)?;

                if !autocommit {
                    let temp_old_key = key::temp_data(txid, old_key);
                    let temp_new_key = key::temp_data_str(txid, &new_key);

                    tree.insert(temp_old_key, old_key)?;
                    tree.insert(temp_new_key, new_key.as_bytes())?;
                }
            }

            if !autocommit {
                let temp_old_key = key::temp_schema(txid, table_name);
                let temp_new_key = key::temp_schema(txid, new_table_name);

                tree.insert(temp_old_key, old_schema_key.as_bytes())?;
                tree.insert(temp_new_key, new_schema_key.as_bytes())?;
            }

            Ok(TxPayload::Success)
        });

        if self.check_retry(tx_result)? {
            self.rename_schema(table_name, new_table_name).await?;
        }

        Ok(())
    }

    async fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let (schema_key, snapshot) = fetch_schema(tree, table_name)?;
            let snapshot = snapshot
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let Schema {
                column_defs,
                indexes,
                engine,
                ..
            } = snapshot
                .get(txid, None)
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let column_defs = column_defs
                .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            if column_defs
                .iter()
                .any(|ColumnDef { name, .. }| name == new_column_name)
            {
                return Err(
                    AlterTableError::AlreadyExistingColumn(new_column_name.to_owned()).into(),
                )
                .map_err(ConflictableTransactionError::Abort);
            }

            let i = column_defs
                .iter()
                .position(|column_def| column_def.name == old_column_name)
                .ok_or_else(|| AlterTableError::RenamingColumnNotFound.into())
                .map_err(ConflictableTransactionError::Abort)?;

            let ColumnDef {
                data_type,
                nullable,
                default,
                unique,
                ..
            } = column_defs[i].clone();

            let column_def = ColumnDef {
                name: new_column_name.to_owned(),
                data_type,
                nullable,
                default,
                unique,
            };
            let column_defs = Vector::from(column_defs).update(i, column_def).into();

            let schema = Schema {
                table_name: table_name.to_owned(),
                column_defs: Some(column_defs),
                indexes,
                engine,
            };
            let (snapshot, _) = snapshot.update(txid, schema);
            let value = bincode::serialize(&snapshot)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;
            tree.insert(schema_key.as_bytes(), value)?;

            if !autocommit {
                let temp_key = key::temp_schema(txid, table_name);

                tree.insert(temp_key, schema_key.as_bytes())?;
            }

            Ok(TxPayload::Success)
        });

        if self.check_retry(tx_result)? {
            self.rename_column(table_name, old_column_name, new_column_name)
                .await?;
        }

        Ok(())
    }

    async fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()> {
        let prefix = format!("data/{}/", table_name);
        let items = self
            .tree
            .scan_prefix(prefix.as_bytes())
            .map(|item| item.map_err(err_into))
            .collect::<Result<Vec<_>>>()?;

        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let (schema_key, schema_snapshot) = fetch_schema(tree, table_name)?;
            let schema_snapshot = schema_snapshot
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let Schema {
                table_name,
                column_defs,
                indexes,
                engine,
                ..
            } = schema_snapshot
                .get(txid, None)
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let column_defs = column_defs
                .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            if column_defs
                .iter()
                .any(|ColumnDef { name, .. }| name == &column_def.name)
            {
                let adding_column = column_def.name.to_owned();

                return Err(AlterTableError::AlreadyExistingColumn(adding_column).into())
                    .map_err(ConflictableTransactionError::Abort);
            }

            let ColumnDef {
                data_type,
                nullable,
                default,
                ..
            } = column_def;

            let value = match (default, nullable) {
                (Some(expr), _) => {
                    let evaluated = block_on(evaluate_stateless(None, expr))
                        .map_err(ConflictableTransactionError::Abort)?;

                    evaluated
                        .try_into_value(data_type, *nullable)
                        .map_err(ConflictableTransactionError::Abort)?
                }
                (None, true) => Value::Null,
                (None, false) => {
                    return Err(AlterTableError::DefaultValueRequired(column_def.clone()).into())
                        .map_err(ConflictableTransactionError::Abort);
                }
            };

            // migrate data
            for (key, snapshot) in items.iter() {
                let snapshot: Snapshot<DataRow> = bincode::deserialize(snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;
                let row = match snapshot.clone().extract(txid, None) {
                    Some(row) => row,
                    None => {
                        continue;
                    }
                };

                let values = match row {
                    DataRow::Vec(values) => values,
                    DataRow::Map(_) => {
                        return Err(Error::StorageMsg(
                            "conflict - add_column failed: schemaless row found".to_owned(),
                        ))
                        .map_err(ConflictableTransactionError::Abort);
                    }
                };
                let row = values
                    .into_iter()
                    .chain(once(value.clone()))
                    .collect::<Vec<Value>>()
                    .into();

                let (snapshot, _) = snapshot.update(txid, row);
                let snapshot = bincode::serialize(&snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                tree.insert(key, snapshot)?;

                if !autocommit {
                    let temp_key = key::temp_data(txid, key);

                    tree.insert(temp_key, key)?;
                }
            }

            // update schema
            let column_defs = column_defs
                .into_iter()
                .chain(once(column_def.clone()))
                .collect::<Vec<ColumnDef>>();

            let temp_key = key::temp_schema(txid, &table_name);

            let schema = Schema {
                table_name,
                column_defs: Some(column_defs),
                indexes,
                engine,
            };
            let (schema_snapshot, _) = schema_snapshot.update(txid, schema);
            let schema_value = bincode::serialize(&schema_snapshot)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            tree.insert(schema_key.as_bytes(), schema_value)?;

            if !autocommit {
                tree.insert(temp_key, schema_key.as_bytes())?;
            }

            Ok(TxPayload::Success)
        });

        if self.check_retry(tx_result)? {
            self.add_column(table_name, column_def).await?;
        }

        Ok(())
    }

    async fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let prefix = format!("data/{}/", table_name);
        let items = self
            .tree
            .scan_prefix(prefix.as_bytes())
            .map(|item| item.map_err(err_into))
            .collect::<Result<Vec<_>>>()?;

        let state = &self.state;
        let tx_timeout = self.tx_timeout;
        let tx_result = self.tree.transaction(move |tree| {
            let (txid, autocommit) = match lock::acquire(tree, state, tx_timeout)? {
                LockAcquired::Success { txid, autocommit } => (txid, autocommit),
                LockAcquired::RollbackAndRetry { lock_txid } => {
                    return Ok(TxPayload::RollbackAndRetry(lock_txid));
                }
            };

            let (schema_key, schema_snapshot) = fetch_schema(tree, table_name)?;
            let schema_snapshot = schema_snapshot
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let Schema {
                table_name,
                column_defs,
                indexes,
                engine,
                ..
            } = schema_snapshot
                .get(txid, None)
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let column_defs = column_defs
                .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()).into())
                .map_err(ConflictableTransactionError::Abort)?;

            let column_index = column_defs
                .iter()
                .position(|ColumnDef { name, .. }| name == column_name);
            let column_index = match (column_index, if_exists) {
                (Some(index), _) => index,
                (None, true) => {
                    return Ok(TxPayload::Success);
                }
                (None, false) => {
                    return Err(
                        AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into(),
                    )
                    .map_err(ConflictableTransactionError::Abort);
                }
            };

            // migrate data
            for (key, snapshot) in items.iter() {
                let snapshot: Snapshot<DataRow> = bincode::deserialize(snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;
                let row = match snapshot.clone().extract(txid, None) {
                    Some(row) => row,
                    None => {
                        continue;
                    }
                };

                let values = match row {
                    DataRow::Vec(values) => values,
                    DataRow::Map(_) => {
                        return Err(Error::StorageMsg(
                            "conflict - drop_column failed: schemaless row found".to_owned(),
                        ))
                        .map_err(ConflictableTransactionError::Abort);
                    }
                };

                let row = values
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, v)| (i != column_index).then_some(v))
                    .collect::<Vec<_>>()
                    .into();

                let (snapshot, _) = snapshot.update(txid, row);
                let snapshot = bincode::serialize(&snapshot)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                tree.insert(key, snapshot)?;

                if !autocommit {
                    let temp_key = key::temp_data(txid, key);

                    tree.insert(temp_key, key)?;
                }
            }

            // update schema
            let column_defs = column_defs
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| (i != column_index).then_some(v))
                .collect::<Vec<ColumnDef>>();

            let temp_key = key::temp_schema(txid, &table_name);

            let schema = Schema {
                table_name,
                column_defs: Some(column_defs),
                indexes,
                engine,
            };
            let (schema_snapshot, _) = schema_snapshot.update(txid, schema);
            let schema_value = bincode::serialize(&schema_snapshot)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;
            tree.insert(schema_key.as_bytes(), schema_value)?;

            if !autocommit {
                tree.insert(temp_key, schema_key.as_bytes())?;
            }

            Ok(TxPayload::Success)
        });

        if self.check_retry(tx_result)? {
            self.drop_column(table_name, column_name, if_exists).await?;
        }

        Ok(())
    }
}
