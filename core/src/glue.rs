use crate::{
    executor::{Payload, execute},
    parse_sql::parse,
    plan::StatementPlan,
    result::Result,
    store::{GStore, GStoreMut, Planner},
    translate::{IntoParamLiteral, ParamLiteral, translate_with_params},
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
    pub fn plan_with_params<Sql, I, P>(&mut self, sql: Sql, params: I) -> Result<Vec<StatementPlan>>
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
        parsed
            .into_iter()
            .map(|p| {
                translate_with_params(&p, &params)
                    .and_then(|statement| self.storage.plan(statement.into()))
            })
            .collect()
    }

    /// Plans all statements in the SQL string without parameters.
    ///
    /// # Errors
    ///
    /// Returns an error when parsing the SQL text fails or when planning one of the
    /// statements fails.
    pub fn plan<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<StatementPlan>> {
        self.plan_with_params(sql, std::iter::empty::<ParamLiteral>())
    }

    pub fn execute_stmt(&mut self, statement: &StatementPlan) -> Result<Payload> {
        execute(&mut self.storage, statement)
    }

    /// Executes all statements in the SQL string using the supplied parameters.
    ///
    /// # Errors
    ///
    /// Returns an error when parsing fails, planning fails, or executing a statement
    /// against the storage fails.
    pub fn execute_with_params<Sql, I, P>(&mut self, sql: Sql, params: I) -> Result<Vec<Payload>>
    where
        Sql: AsRef<str>,
        I: IntoIterator<Item = P>,
        P: IntoParamLiteral,
    {
        let statements = self.plan_with_params(sql, params)?;
        let mut payloads = Vec::<Payload>::new();
        for statement in &statements {
            let payload = self.execute_stmt(statement)?;
            payloads.push(payload);
        }

        Ok(payloads)
    }

    /// Executes all statements in the SQL string without parameters.
    ///
    /// # Errors
    ///
    /// Returns an error when parsing fails, planning fails, or executing a statement fails.
    pub fn execute<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Payload>> {
        self.execute_with_params(sql, std::iter::empty::<ParamLiteral>())
    }
}
