#![cfg(feature = "transaction")]

use {
    super::JsonlStorage,
    async_trait::async_trait,
    gluesql_core::{
        result::{Error, MutResult},
        store::Transaction,
    },
};

#[async_trait(?Send)]
impl Transaction for JsonlStorage {
    async fn begin(self, autocommit: bool) -> MutResult<Self, bool> {
        if autocommit {
            return Ok((self, false));
        }

        Err((
            self,
            Error::StorageMsg("[JsonlStorage] transaction is not supported".to_owned()),
        ))
    }

    async fn rollback(self) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[JsonlStorage] transaction is not supported".to_owned()),
        ))
    }

    async fn commit(self) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[JsonlStorage] transaction is not supported".to_owned()),
        ))
    }
}
