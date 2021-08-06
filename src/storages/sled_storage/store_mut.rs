use {
    super::{
        err_into, error::StorageError, index_sync::IndexSync, key, lock, SledStorage, Snapshot,
    },
    crate::{
        data::{Row, Schema},
        result::Result,
        IndexError, MutResult, StoreMut,
    },
    async_trait::async_trait,
    sled::{
        transaction::{ConflictableTransactionError, TransactionError},
        IVec,
    },
};

macro_rules! transaction {
    ($self: expr, $expr: expr) => {{
        let result = $self.tree.transaction($expr).map_err(|e| match e {
            TransactionError::Abort(e) => e,
            TransactionError::Storage(e) => StorageError::Sled(e).into(),
        });

        match result {
            Ok(_) => Ok(($self, ())),
            Err(e) => Err(($self, e)),
        }
    }};
}

#[async_trait(?Send)]
impl StoreMut<IVec> for SledStorage {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let state = &self.state;

        transaction!(self, move |tree| {
            let (txid, _) = lock::acquire(tree, state)?;

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

            Ok(())
        })
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

        transaction!(self, move |tree| {
            let (txid, _) = lock::acquire(tree, state)?;

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
                    return Ok(());
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

            Ok(())
        })
    }

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        let state = &self.state;

        transaction!(self, move |tree| {
            let (txid, autocommit) = lock::acquire(tree, state)?;
            let index_sync = IndexSync::new(tree, txid, table_name)?;

            for row in rows.iter() {
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

            Ok(())
        })
    }

    async fn update_data(self, table_name: &str, rows: Vec<(IVec, Row)>) -> MutResult<Self, ()> {
        let state = &self.state;

        transaction!(self, move |tree| {
            let (txid, autocommit) = lock::acquire(tree, state)?;
            let index_sync = IndexSync::new(tree, txid, table_name)?;

            for (key, new_row) in rows.iter() {
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

            Ok(())
        })
    }

    async fn delete_data(self, table_name: &str, keys: Vec<IVec>) -> MutResult<Self, ()> {
        let state = &self.state;

        transaction!(self, |tree| {
            let (txid, autocommit) = lock::acquire(tree, state)?;
            let index_sync = IndexSync::new(tree, txid, table_name)?;

            for key in keys.iter() {
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

            Ok(())
        })
    }
}
