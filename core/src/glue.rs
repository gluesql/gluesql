use {
    crate::{
        ast::Statement,
        executor::{execute, Payload},
        parse_sql::parse,
        plan::plan,
        result::Result,
        store::{GStore, GStoreMut},
        translate::translate,
    },
    futures::executor::block_on,
    std::marker::PhantomData,
};

pub struct Glue<T, U: GStore<T> + GStoreMut<T>> {
    _marker: PhantomData<T>,
    pub storage: Option<U>,
}

impl<T, U: GStore<T> + GStoreMut<T>> Glue<T, U> {
    pub fn new(storage: U) -> Self {
        let storage = Some(storage);

        Self {
            _marker: PhantomData,
            storage,
        }
    }

    pub async fn plan(&self, sql: &str) -> Result<Statement> {
        let parsed = parse(sql)?;
        let statement = translate(&parsed[0])?;
        let storage = self.storage.as_ref().unwrap();

        plan(storage, statement).await
    }

    pub fn execute_stmt(&mut self, statement: Statement) -> Result<Payload> {
        block_on(self.execute_stmt_async(statement))
    }

    pub fn execute(&mut self, sql: &str) -> Result<Payload> {
        let statement = block_on(self.plan(sql))?;

        self.execute_stmt(statement)
    }

    pub async fn execute_stmt_async(&mut self, statement: Statement) -> Result<Payload> {
        let storage = self.storage.take().unwrap();

        match execute(storage, &statement).await {
            Ok((storage, payload)) => {
                self.storage = Some(storage);

                Ok(payload)
            }
            Err((storage, error)) => {
                self.storage = Some(storage);

                Err(error)
            }
        }
    }

    pub async fn execute_async(&mut self, sql: &str) -> Result<Payload> {
        let statement = self.plan(sql).await?;

        self.execute_stmt_async(statement).await
    }
}
