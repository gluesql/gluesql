use {
    super::SharedMemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        result::{Error, MutResult},
        store::Transaction,
    },
};

#[async_trait(?Send)]
impl Transaction for SharedMemoryStorage {
    async fn begin(self, autocommit: bool) -> MutResult<Self, bool> {
        if autocommit {
            return Ok((self, false));
        }

        Err((
            self,
            Error::StorageMsg("[Shared MemoryStorage] transaction is not supported".to_owned()),
        ))
    }

    async fn rollback(self) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[Shared MemoryStorage] transaction is not supported".to_owned()),
        ))
    }

    async fn commit(self) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[Shared MemoryStorage] transaction is not supported".to_owned()),
        ))
    }
}
