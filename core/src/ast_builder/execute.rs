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

#[async_trait(?Send)]
pub trait Execute<T: GStore + GStoreMut>
where
    Self: Sized + Build,
{
    async fn execute(self, glue: &mut Glue<T>) -> Result<Payload> {
        let statement = self.build()?;

        glue.execute_stmt_async(&statement).await
    }
}

#[async_trait(?Send)]
impl<T: GStore + GStoreMut, B: Build> Execute<T> for B {}

impl Build for Statement {
    fn build(self) -> Result<Statement> {
        Ok(self)
    }
}
