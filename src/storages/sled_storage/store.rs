use async_trait::async_trait;
use sled::IVec;

use super::{fetch_schema, SledStorage, StorageError};
use crate::try_into;
use crate::{Error, MutResult, Result, Row, RowIter, Schema, Store, StoreMut};

#[async_trait]
impl StoreMut<IVec> for SledStorage {
    fn generate_id(self, table_name: &str) -> MutResult<Self, IVec> {
        let id = try_into!(self, self.tree.generate_id());
        let id = format!("data/{}/{}", table_name, id);

        Ok((self, IVec::from(id.as_bytes())))
    }

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

    fn insert_data(self, key: &IVec, row: Row) -> MutResult<Self, ()> {
        let value = try_into!(self, bincode::serialize(&row));

        try_into!(self, self.tree.insert(key, value));

        Ok((self, ()))
    }

    fn delete_data(self, key: &IVec) -> MutResult<Self, ()> {
        try_into!(self, self.tree.remove(key));

        Ok((self, ()))
    }
}

impl Store<IVec> for SledStorage {
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        fetch_schema(&self.tree, table_name).map(|(_, schema)| schema)
    }

    fn scan_data(&self, table_name: &str) -> Result<RowIter<IVec>> {
        let prefix = format!("data/{}/", table_name);

        let result_set = self.tree.scan_prefix(prefix.as_bytes()).map(move |item| {
            let (key, value) = try_into!(item);
            let value = try_into!(bincode::deserialize(&value));

            Ok((key, value))
        });

        Ok(Box::new(result_set))
    }
}
