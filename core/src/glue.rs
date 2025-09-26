use {
    crate::{
        ast::Statement,
        executor::{Payload, execute},
        parse_sql::parse,
        plan::plan,
        result::Result,
        store::{GStore, GStoreMut},
        translate::{ParamLiteral, translate_with_params},
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

    pub async fn plan_with_params<Sql, I, P>(
        &mut self,
        sql: Sql,
        params: I,
    ) -> Result<Vec<Statement>>
    where
        Sql: AsRef<str>,
        I: IntoIterator<Item = P>,
        P: Into<ParamLiteral>,
    {
        let parsed = parse(sql)?;
        let params: Vec<ParamLiteral> = params.into_iter().map(Into::into).collect();
        let storage = &self.storage;
        stream::iter(parsed)
            .map(|p| translate_with_params(&p, params.clone()))
            .then(|statement| async move { plan(storage, statement?).await })
            .try_collect()
            .await
    }

    pub async fn plan<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Statement>> {
        self.plan_with_params(sql, std::iter::empty::<ParamLiteral>())
            .await
    }

    pub async fn execute_stmt(&mut self, statement: &Statement) -> Result<Payload> {
        execute(&mut self.storage, statement).await
    }

    pub async fn execute_with_params<Sql, I, P>(
        &mut self,
        sql: Sql,
        params: I,
    ) -> Result<Vec<Payload>>
    where
        Sql: AsRef<str>,
        I: IntoIterator<Item = P>,
        P: Into<ParamLiteral>,
    {
        let statements = self.plan_with_params(sql, params).await?;
        let mut payloads = Vec::<Payload>::new();
        for statement in statements.iter() {
            let payload = self.execute_stmt(statement).await?;
            payloads.push(payload);
        }

        Ok(payloads)
    }

    pub async fn execute<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Payload>> {
        self.execute_with_params(sql, std::iter::empty::<ParamLiteral>())
            .await
    }
}
