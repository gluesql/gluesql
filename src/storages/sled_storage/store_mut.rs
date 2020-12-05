use async_trait::async_trait;
use sled::IVec;

use super::{err_into, SledStorage};
use crate::{MutResult, Row, Schema, StoreMut};

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
        for row in rows.into_iter() {
            let id = try_into!(self, self.tree.generate_id());
            let id = id.to_be_bytes();
            let prefix = format!("data/{}/", table_name);

            let bytes = prefix
                .into_bytes()
                .into_iter()
                .chain(id.iter().copied())
                .collect::<Vec<_>>();

            let key = IVec::from(bytes);
            let value = try_into!(self, bincode::serialize(&row));

            try_into!(self, self.tree.insert(key, value));
        }

        Ok((self, ()))
    }

    async fn update_data(self, rows: Vec<(IVec, Row)>) -> MutResult<Self, ()> {
        for (key, row) in rows.into_iter() {
            let value = try_into!(self, bincode::serialize(&row));

            try_into!(self, self.tree.insert(key, value));
        }

        Ok((self, ()))
    }

    async fn delete_data(self, key: &IVec) -> MutResult<Self, ()> {
        try_into!(self, self.tree.remove(key));

        Ok((self, ()))
    }
}
