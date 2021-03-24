#![cfg(feature = "auto-increment")]
use {
    super::{
        error::{err_into, StorageError},
        SledStorage,
    },
    crate::{AutoIncrement, MutResult, Value},
    async_trait::async_trait,
    fstrings::*,
    sled::transaction::{ConflictableTransactionError, TransactionError},
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
macro_rules! try_into {
    ($self: expr, $expr: expr) => {
        match $expr {
            Err(e) => {
                return Err(($self, e));
            }
            Ok(v) => v,
        }
    };
}

#[async_trait(?Send)]
impl AutoIncrement for SledStorage {
    async fn generate_value(self, table_name: &str, column_name: &str) -> MutResult<Self, Value> {
        let value = try_into!(
            self,
            self.tree
                .get(f!("generator/{table_name}/{column_name}").as_bytes())
                .map_err(err_into)
        );

        let value = if let Some(value) = value {
            bincode::deserialize(&value).ok()
        } else {
            None
        };
        let value = match value {
            Some(Value::I64(value)) => value,
            _ => 1,
        };
        let next_value = Value::I64(value + 1);
        let value = Value::I64(value);

        let (self, _) = transaction!(self, |tree| {
            let key = f!("generator/{table_name}/{column_name}");
            let key = key.as_bytes();
            let next_value = bincode::serialize(&next_value)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;
            tree.insert(key, next_value)?;
            Ok(())
        })?;

        Ok((self, value))
    }
}
