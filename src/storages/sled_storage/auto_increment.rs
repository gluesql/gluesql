#![cfg(feature = "auto-increment")]
use {
    super::{error::err_into, SledStorage},
    crate::{AutoIncrement, MutResult, Result},
    async_trait::async_trait,
    fstrings::*,
    std::ops::Range,
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
    async fn get_increment_value(&self, table_name: &str, column_name: &str) -> Result<i64> {
        Ok(self
            .tree
            .get(f!("generator/{table_name}/{column_name}").as_bytes())
            .map_err(err_into)?
            .map(|data| bincode::deserialize(&data).ok())
            .flatten()
            .unwrap_or(1))
    }

    async fn set_increment_value(
        self,
        table_name: &str,
        column_name: &str,
        end: i64,
    ) -> MutResult<Self, ()> {
        let end_ivec = try_into!(self, bincode::serialize(&end).map_err(err_into));

        let key = f!("generator/{table_name}/{column_name}");
        let key = key.as_bytes();

        try_into!(self, self.tree.insert(key, end_ivec).map_err(err_into));

        Ok((self, ()))
    }
}
