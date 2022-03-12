use {
    crate::result::{Error, MutResult},
    async_trait::async_trait,
};

#[async_trait(?Send)]
pub trait Transaction
where
    Self: Sized,
{
    async fn begin(self, autocommit: bool) -> MutResult<Self, bool> {
        if autocommit {
            return Ok((self, false));
        }

        Err((
            self,
            Error::StorageMsg("[Storage] Transaction::begin is not supported".to_owned()),
        ))
    }

    async fn rollback(self) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[Storage] Transaction::rollback is not supported".to_owned()),
        ))
    }

    async fn commit(self) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[Storage] Transaction::commit is not supported".to_owned()),
        ))
    }
}
