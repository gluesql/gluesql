#![cfg(feature = "auto-increment")]
use {
    super::{error::err_into, SledStorage},
    crate::{AutoIncrement, MutResult, Value},
    async_trait::async_trait,
    fstrings::*,
};

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

        const ONE: Value = Value::I64(1);
        let value = value
            .map(|value| bincode::deserialize(&value).ok())
            .flatten()
            .unwrap_or(ONE);
        let next_value = try_into!(self, value.add(&ONE));

        let key = f!("generator/{table_name}/{column_name}");
        let key = key.as_bytes();
        let next_value = try_into!(self, bincode::serialize(&next_value).map_err(err_into));
        try_into!(self, self.tree.insert(key, next_value).map_err(err_into));

        Ok((self, value))
    }
}
