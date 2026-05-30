use {
    super::Build,
    crate::{
        ast::Statement,
        executor::Payload,
        plan::StatementPlan,
        prelude::Glue,
        result::Result,
        store::{GStore, GStoreMut, Planner},
    },
    async_trait::async_trait,
};

#[async_trait]
pub trait Execute<T: GStore + GStoreMut + Planner>
where
    Self: Sized + Build,
{
    async fn execute(self, glue: &mut Glue<T>) -> Result<Payload> {
        let statement = self.build()?;
        let statement = glue.storage.plan(statement).await?;

        glue.execute_stmt(&statement).await
    }
}

impl<T: GStore + GStoreMut + Planner, B: Build> Execute<T> for B {}
#[async_trait]

impl Build for Statement {
    fn build(self) -> Result<StatementPlan> {
        Ok(self.into())
    }
}

impl Build for StatementPlan {
    fn build(self) -> Result<StatementPlan> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{ast_builder::Build, parse_sql::parse, plan::StatementPlan, translate::translate},
        pretty_assertions::assert_eq,
    };

    #[test]
    fn builds_statement_plan_from_statement() {
        let statement = translate(&parse("SELECT 1").unwrap()[0]).unwrap();
        let expected = StatementPlan::from(statement.clone());

        assert_eq!(statement.build(), Ok(expected));
    }
}
