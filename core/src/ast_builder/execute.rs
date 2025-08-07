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

#[async_trait]
pub trait Execute<T: GStore + GStoreMut>
where
    Self: Sized + Build,
{
    async fn execute(self, glue: &mut Glue<T>) -> Result<Payload> {
        let statement = self.build()?;

        glue.execute_stmt(&statement).await
    }
}

impl<T: GStore + GStoreMut, B: Build> Execute<T> for B {}
#[async_trait]

impl Build for Statement {
    fn build(self) -> Result<Statement> {
        Ok(self)
    }
}
