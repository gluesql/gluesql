use {
    crate::{
        ast::Statement,
        executor::{Payload, execute},
        parse_sql::parse,
        plan::plan,
        result::Result,
        store::{GStore, GStoreMut},
        translate::translate,
    },
    futures::{
        TryStreamExt,
        stream::{self, StreamExt},
    },
};

#[derive(Debug)]
pub struct Glue<T: GStore + GStoreMut> {
    pub storage: T,
}

impl<T: GStore + GStoreMut> Glue<T> {
    pub fn new(storage: T) -> Self {
        Self { storage }
    }

    pub async fn plan<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Statement>> {
        let parsed = parse(sql)?;
        let storage = &self.storage;
        stream::iter(parsed)
            .map(|p| translate(&p))
            .then(|statement| async move { plan(storage, statement?).await })
            .try_collect()
            .await
    }

    pub async fn execute_stmt(&mut self, statement: &Statement) -> Result<Payload> {
        execute(&mut self.storage, statement).await
    }

    pub async fn execute<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Payload>> {
        let statements = self.plan(sql).await?;
        let mut payloads = Vec::<Payload>::new();
        for statement in statements.iter() {
            let payload = self.execute_stmt(statement).await?;
            payloads.push(payload);
        }

        Ok(payloads)
    }
}
