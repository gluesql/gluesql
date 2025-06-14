use {
    super::Build,
    crate::{
        ast::Statement,
        executor::Payload,
        prelude::Glue,
        result::Result,
        shared::SendSync,
        store::{GStore, GStoreMut},
    },
    async_trait::async_trait,
};

#[cfg_attr(feature = "send", async_trait)]
#[cfg_attr(not(feature = "send"), async_trait(?Send))]
pub trait Execute<T: GStore + GStoreMut + SendSync>
where
    Self: Sized + Build,
{
    async fn execute(self, glue: &mut Glue<T>) -> Result<Payload> {
        let statement = self.build()?;

        glue.execute_stmt(&statement).await
    }
}

#[cfg_attr(feature = "send", async_trait)]
#[cfg_attr(not(feature = "send"), async_trait(?Send))]
impl<T: GStore + GStoreMut + SendSync, B: Build> Execute<T> for B {}

impl Build for Statement {
    fn build(self) -> Result<Statement> {
        Ok(self)
    }
}
