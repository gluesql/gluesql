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
        let parsed = parse(sql)?;
        let params: Vec<ParamLiteral> = params
            .into_iter()
            .map(IntoParamLiteral::into_param_literal)
            .collect();
        let mut payloads = Vec::<Payload>::new();

        for parsed in parsed {
            let statement = translate_with_params(&parsed, &params)?;
            let statement = self.storage.plan(statement.into())?;
            payloads.push(self.execute_stmt(&statement)?);
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

#[cfg(test)]
mod tests {
    use {
        super::Glue,
        crate::{executor::InsertError, mock::MockStorage, result::Error},
    };

    #[test]
    fn execute_plans_insert_after_create_in_the_same_script() {
        let mut glue = Glue::new(MockStorage::default());
        let result = glue.execute(
            "
            CREATE TABLE greet (name TEXT);
            INSERT INTO greet VALUES ('World');
            ",
        );

        assert!(
            !matches!(result, Err(Error::Insert(InsertError::TableNotFound(_)))),
            "{result:?}"
        );
    }
}
