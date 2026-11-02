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
};

pub trait Execute<T: GStore + GStoreMut + Planner>
where
    Self: Sized + Build,
{
    fn execute(self, glue: &mut Glue<T>) -> Result<Payload> {
        let statement = self.build()?;
        let statement = glue.storage.plan(statement)?;

        glue.execute_stmt(&statement)
    }
}

impl<T: GStore + GStoreMut + Planner, B: Build> Execute<T> for B {}

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
        crate::{
            data::Value,
            executor::Payload,
            glue::Glue,
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::StatementPlan,
            query_builder::{Build, Execute},
            translate::translate,
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn builds_statement_plan_from_statement() {
        let statement = translate(&parse("SELECT 1").unwrap()[0]).unwrap();
        let expected = StatementPlan::from(statement.clone());

        assert_eq!(statement.build(), Ok(expected));
    }

    #[test]
    fn executes_prepared_statement() {
        let mut glue = Glue::new(run("CREATE FUNCTION one() RETURN 1"));
        let sql = "VALUES (one())";
        let statement = glue.plan(sql).unwrap().remove(0);

        let mut glue = Glue::new(MockStorage::default());
        let actual = statement.execute(&mut glue);
        let expected = Ok(Payload::Select {
            labels: vec!["column1".to_owned()],
            rows: vec![vec![Value::I64(1)]],
        });
        assert_eq!(
            actual, expected,
            "execute a prepared statement without stored definitions: {sql}"
        );
    }
}
