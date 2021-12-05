use {crate::result::MutResult, async_trait::async_trait};

#[async_trait(?Send)]
pub trait Transaction
where
    Self: Sized,
{
    async fn begin(self, autocommit: bool) -> MutResult<Self, bool>;

    async fn rollback(self) -> MutResult<Self, ()>;

    async fn commit(self) -> MutResult<Self, ()>;
}
