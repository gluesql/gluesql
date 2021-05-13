use {
    super::{err_into, error::StorageError, SledStorage},
    crate::{MutResult, Row, Schema, StoreMut},
    async_trait::async_trait,
    sled::{
        transaction::{ConflictableTransactionError, TransactionError},
        IVec,
    },
};

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
        transaction!(self, |tree| {
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

                tree.insert(key, value)?;
            }

            Ok(())
        })
    }

    async fn update_data(self, rows: Vec<(IVec, Row)>) -> MutResult<Self, ()> {
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

    async fn delete_data(self, keys: Vec<IVec>) -> MutResult<Self, ()> {
        transaction!(self, |tree| {
            for key in keys.iter() {
                tree.remove(key)?;
            }

            Ok(())
        })
    }
}
