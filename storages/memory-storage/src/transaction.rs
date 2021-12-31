use {
    super::MemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        result::{Error, MutResult},
        store::Transaction,
    },
};

#[async_trait(?Send)]
impl Transaction for MemoryStorage {
    async fn begin(self, autocommit: bool) -> MutResult<Self, bool> {
        if autocommit {
            return Ok((self, false));
        }

        Err((
            self,
            Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned()),
        ))
    }

    async fn rollback(self) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned()),
        ))
    }

    async fn commit(self) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned()),
        ))
    }
}
