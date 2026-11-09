use {
    crate::{
        executor::{Payload, execute},
        parse_sql::parse,
        plan::{StatementPlan, StatementPlanKind},
        result::Result,
        store::{GStore, GStoreMut, Planner},
        translate::{IntoParamLiteral, ParamLiteral, translate_with_params},
    },
    std::borrow::Borrow,
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

    /// Executes a prepared statement with automatic transaction handling, without planning.
    pub fn execute_stmt(&mut self, statement: &StatementPlan) -> Result<Payload> {
        self.with_transaction(execute, statement)
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
            payloads.push(self.plan_and_execute(statement.into())?);
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

    pub(crate) fn plan_and_execute(&mut self, statement: StatementPlan) -> Result<Payload> {
        self.with_transaction(
            |storage, statement| {
                let statement = storage.plan(statement)?;

                execute(storage, &statement)
            },
            statement,
        )
    }

    fn with_transaction<S: Borrow<StatementPlan>>(
        &mut self,
        operation: impl FnOnce(&mut T, S) -> Result<Payload>,
        statement: S,
    ) -> Result<Payload> {
        if matches!(
            &statement.borrow().kind,
            StatementPlanKind::StartTransaction
                | StatementPlanKind::Rollback
                | StatementPlanKind::Commit
        ) {
            return operation(&mut self.storage, statement);
        }

        let autocommit = self.storage.begin(true)?;
        let result = operation(&mut self.storage, statement);

        if !autocommit {
            return result;
        }

        match result {
            Ok(payload) => self.storage.commit().map(|()| payload),
            Err(error) => {
                self.storage.rollback()?;

                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Glue,
        crate::{mock::MockStorage, result::Error},
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

        assert_eq!(
            result,
            Err(Error::StorageMsg(
                "[Storage] StoreMut::append_data is not supported".to_owned(),
            ))
        );
    }
}
