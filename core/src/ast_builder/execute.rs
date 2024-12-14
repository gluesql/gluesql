use {
    super::Build,
    crate::{
        ast::Statement,
        executor::Payload,
        prelude::Glue,
        result::Result,
        store::{GStore, GStoreMut},
    },
    async_trait::async_trait,
};

#[cfg_attr(not(feature = "send"), async_trait(?Send))]
#[cfg_attr(feature = "send", async_trait)]
pub trait Execute<
    #[cfg(feature = "send")] T: GStore + GStoreMut + Send + Sync,
    #[cfg(not(feature = "send"))] T: GStore + GStoreMut,
> where
    Self: Sized + Build,
{
    async fn execute(self, glue: &mut Glue<T>) -> Result<Payload> {
        let statement = self.build()?;

        glue.execute_stmt(&statement).await
    }
}

#[cfg_attr(not(feature = "send"), async_trait(?Send))]
#[cfg_attr(feature = "send", async_trait)]
impl<
        #[cfg(feature = "send")] T: GStore + GStoreMut + Send + Sync,
        #[cfg(not(feature = "send"))] T: GStore + GStoreMut,
        B: Build,
    > Execute<T> for B
{
}

impl Build for Statement {
    fn build(self) -> Result<Statement> {
        Ok(self)
    }
}
