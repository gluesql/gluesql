#![cfg(feature = "auto-increment")]
use {
    super::{error::err_into, SledStorage},
    crate::{AutoIncrement, MutResult},
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
    async fn generate_values(
        self,
        table_name: &str,
        column_name: &str,
        size: usize,
    ) -> MutResult<Self, Range<i64>> {
        let start = try_into!(
            self,
            self.tree
                .get(f!("generator/{table_name}/{column_name}").as_bytes())
                .map_err(err_into)
        )
        .map(|data| bincode::deserialize(&data).ok())
        .flatten()
        .unwrap_or(1);

        let end = start + (size as i64);
        let end_ivec = try_into!(self, bincode::serialize(&end).map_err(err_into));

        let key = f!("generator/{table_name}/{column_name}");
        let key = key.as_bytes();

        try_into!(self, self.tree.insert(key, end_ivec).map_err(err_into));

        Ok((self, Range { start, end }))
    }
}
