use {
    crate::{
        ast::Statement,
        executor::{Payload, execute},
        parse_sql::parse,
        result::Result,
        store::{GStore, GStoreMut, Planner},
        translate::{IntoParamLiteral, ParamLiteral, translate_with_params},
    },
    futures::{
        TryStreamExt,
        stream::{self, StreamExt},
    },
};

#[derive(Debug)]
pub struct Glue<T: GStore + GStoreMut + Planner> {
    pub storage: T,
}

impl<T: GStore + GStoreMut + Planner> Glue<T> {
    pub fn new(storage: T) -> Self {
        Self { storage }
    }

    /// Plans all statements in the SQL string using the supplied parameters.
    ///
    /// # Errors
    ///
    /// Returns an error when parsing the SQL text fails or when building an execution plan for
    /// a statement fails.
    pub async fn plan_with_params<Sql, I, P>(
        &mut self,
        sql: Sql,
        params: I,
    ) -> Result<Vec<Statement>>
    where
        Sql: AsRef<str>,
        I: IntoIterator<Item = P>,
        P: IntoParamLiteral,
    {
        let parsed = parse(sql)?;
        let params: Vec<ParamLiteral> = params
            .into_iter()
            .map(IntoParamLiteral::into_param_literal)
            .collect();
        let storage = &self.storage;
        stream::iter(parsed)
            .map(|p| translate_with_params(&p, &params))
            .then(|statement| async move { storage.plan(statement?).await })
            .try_collect()
            .await
    }

    /// Plans all statements in the SQL string without parameters.
    ///
    /// # Errors
    ///
    /// Returns an error when parsing the SQL text fails or when planning one of the
    /// statements fails.
    pub async fn plan<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Statement>> {
        self.plan_with_params(sql, std::iter::empty::<ParamLiteral>())
            .await
    }

    pub async fn execute_stmt(&mut self, statement: &Statement) -> Result<Payload> {
        execute(&mut self.storage, statement).await
    }

    /// Executes all statements in the SQL string using the supplied parameters.
    ///
    /// # Errors
    ///
    /// Returns an error when parsing fails, planning fails, or executing a statement
    /// against the storage fails.
    pub async fn execute_with_params<Sql, I, P>(
        &mut self,
        sql: Sql,
        params: I,
    ) -> Result<Vec<Payload>>
    where
        Sql: AsRef<str>,
        I: IntoIterator<Item = P>,
        P: IntoParamLiteral,
    {
        let statements = self.plan_with_params(sql, params).await?;
        let mut payloads = Vec::<Payload>::new();
        for statement in &statements {
            let payload = self.execute_stmt(statement).await?;
            payloads.push(payload);
        }

        Ok(payloads)
    }

    /// Executes all statements in the SQL string without parameters.
    ///
    /// # Errors
    ///
    /// Returns an error when parsing fails, planning fails, or executing a statement fails.
    pub async fn execute<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Payload>> {
        self.execute_with_params(sql, std::iter::empty::<ParamLiteral>())
            .await
    }
}
