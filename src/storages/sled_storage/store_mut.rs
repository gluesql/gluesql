use {
    super::{err_into, error::StorageError, SledStorage},
    crate::{MutResult, Row, Schema, StoreMut},
    async_trait::async_trait,
    sled::{
        transaction::{ConflictableTransactionError, TransactionError},
        IVec,
    },
};

#[cfg(feature = "index")]
use {super::index_sync::IndexSync, crate::IndexError};

#[cfg(feature = "index")]
macro_rules! try_self {
    ($self: expr, $expr: expr) => {
        match $expr {
            Err(e) => {
                return Err(($self, e.into()));
            }
            Ok(v) => v,
        }
    };
}

macro_rules! try_into {
    ($self: expr, $expr: expr) => {
        match $expr.map_err(err_into) {
            Err(e) => {
                return Err(($self, e));
            }
            Ok(v) => v,
        }
    };
}

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
        let key = format!("schema/{}", schema.table_name);
        let key = key.as_bytes();
        let value = try_into!(self, bincode::serialize(schema));

        try_into!(self, self.tree.insert(key, value));

        Ok((self, ()))
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let prefix = format!("data/{}/", table_name);
        let tree = &self.tree;

        for item in tree.scan_prefix(prefix.as_bytes()) {
            let (key, _) = try_into!(self, item);

            try_into!(self, tree.remove(key));
        }

        let key = format!("schema/{}", table_name);
        try_into!(self, tree.remove(key));

        Ok((self, ()))
    }

    async fn insert_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        #[cfg(feature = "index")]
        let index_sync = try_self!(self, IndexSync::new(&self.tree, table_name));

        transaction!(self, move |tree| {
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
                let value = bincode::serialize(row)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                #[cfg(feature = "index")]
                index_sync.insert(&tree, &key, row)?;

                tree.insert(key, value)?;
            }

            Ok(())
        })
    }

    #[cfg(not(feature = "index"))]
    async fn update_data(self, _: &str, rows: Vec<(IVec, Row)>) -> MutResult<Self, ()> {
        transaction!(self, |tree| {
            for (key, row) in rows.iter() {
                let value = bincode::serialize(row)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                tree.insert(key, value)?;
            }

            Ok(())
        })
    }

    #[cfg(feature = "index")]
    async fn update_data(self, table_name: &str, rows: Vec<(IVec, Row)>) -> MutResult<Self, ()> {
        let index_sync = try_self!(self, IndexSync::new(&self.tree, table_name));

        transaction!(self, |tree| {
            for (key, new_row) in rows.iter() {
                let new_value = bincode::serialize(new_row)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                let old_value = tree
                    .insert(key, new_value)?
                    .ok_or_else(|| IndexError::ConflictOnEmptyIndexValueUpdate.into())
                    .map_err(ConflictableTransactionError::Abort)?;

                let old_row = bincode::deserialize(&old_value)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                index_sync.update(&tree, &key, &old_row, new_row)?;
            }

            Ok(())
        })
    }

    #[cfg(not(feature = "index"))]
    async fn delete_data(self, _: &str, keys: Vec<IVec>) -> MutResult<Self, ()> {
        transaction!(self, |tree| {
            for key in keys.iter() {
                tree.remove(key)?;
            }

            Ok(())
        })
    }

    #[cfg(feature = "index")]
    async fn delete_data(self, table_name: &str, keys: Vec<IVec>) -> MutResult<Self, ()> {
        let index_sync = try_self!(self, IndexSync::new(&self.tree, table_name));

        transaction!(self, |tree| {
            for key in keys.iter() {
                let value = tree
                    .remove(key)?
                    .ok_or_else(|| IndexError::ConflictOnEmptyIndexValueDelete.into())
                    .map_err(ConflictableTransactionError::Abort)?;

                let row = bincode::deserialize(&value)
                    .map_err(err_into)
                    .map_err(ConflictableTransactionError::Abort)?;

                index_sync.delete(&tree, &key, &row)?;
            }

            Ok(())
        })
    }
}
