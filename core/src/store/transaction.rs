use {crate::result::Result, async_trait::async_trait};

#[async_trait]
pub trait Transaction {
    async fn begin(&mut self, autocommit: bool) -> Result<bool>;

    async fn rollback(&mut self) -> Result<()>;

    async fn commit(&mut self) -> Result<()>;
}
