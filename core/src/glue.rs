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
    futures::{
        executor::block_on,
        stream::{self, StreamExt},
        TryStreamExt,
    },
};

pub struct Glue<T: GStore + GStoreMut> {
    pub storage: Option<T>,
}

impl<T: GStore + GStoreMut> Glue<T> {
    pub fn new(storage: T) -> Self {
        Self {
            storage: Some(storage),
        }
    }

    pub async fn plan<Sql: AsRef<str>>(&self, sql: Sql) -> Result<Vec<Statement>> {
        let parsed = parse(sql)?;
        let storage = self.storage.as_ref().unwrap();
        stream::iter(parsed)
            .map(|p| translate(&p))
            .then(|statement| async move { plan(storage, statement?).await })
            .try_collect()
            .await
    }

    pub fn execute_stmt(&mut self, statement: &Statement) -> Result<Payload> {
        block_on(self.execute_stmt_async(statement))
    }

    pub fn execute<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Payload>> {
        let statements = block_on(self.plan(sql))?;
        statements.iter().map(|s| self.execute_stmt(s)).collect()
    }

    pub async fn execute_stmt_async(&mut self, statement: &Statement) -> Result<Payload> {
        let storage = self.storage.take().unwrap();

        match execute(storage, statement).await {
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

    pub async fn execute_async<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Payload>> {
        let statements = self.plan(sql).await?;
        let mut payloads = Vec::<Payload>::new();
        for statement in statements.iter() {
            let payload = self.execute_stmt_async(statement).await?;
            payloads.push(payload);
        }

        Ok(payloads)
    }
}
