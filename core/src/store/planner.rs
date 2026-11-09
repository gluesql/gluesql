use {
    super::{CustomFunction, Store},
    crate::{
        plan::StatementPlan,
        planner::{
            plan_aggregate, plan_create_table_columns, plan_custom_function_calls, plan_hash_join,
            plan_primary_key, plan_schema_map, plan_schemaless, validate, validate_create_function,
            validate_create_index, validate_insert,
        },
        result::Result,
    },
};

pub trait Planner: Store + CustomFunction {
    fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        let statement = plan_schema_map(self, statement)?;

        validate(&statement)?;
        validate_create_index(&statement)?;

        let statement = plan_schemaless(statement)?;
        let statement = plan_primary_key(statement)?;
        let statement = plan_hash_join(statement)?;
        let statement = plan_aggregate(statement);

        validate_create_function(&statement)?;
        let statement = plan_custom_function_calls(self, statement)?;

        validate_insert(&statement)?;
        plan_create_table_columns(statement)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Planner,
        crate::{
            mock::{MockStorage, run},
            planner::PlannerError,
            query_builder::{Build, primary_key, table},
            result::Error,
        },
    };

    #[test]
    fn plan() {
        let storage = MockStorage::default();
        let ddl = "CREATE TABLE Item (id INT PRIMARY KEY)";
        let input = table("Item")
            .select()
            .filter("id = 7")
            .with_schema(ddl)
            .with_schema("CREATE TABLE Unused (name TEXT)")
            .build()
            .unwrap();
        let actual = storage.plan(input);
        let expected = table("Item")
            .index_by(primary_key().eq("7"))
            .select()
            .with_schema(ddl)
            .with_schema("CREATE TABLE Unused (name TEXT)")
            .build();
        assert_eq!(actual, expected);
    }

    #[test]
    fn plan_error() {
        let storage = MockStorage::default();
        let input = table("Missing")
            .insert()
            .values(vec!["missing()"])
            .build()
            .unwrap();
        let actual = storage.plan(input);
        let expected = Err(Error::Planner(PlannerError::UnsupportedFunction(
            "MISSING".to_owned(),
        )));
        assert_eq!(actual, expected);

        let storage = run("CREATE FUNCTION one() RETURN 1");
        let input = table("Missing")
            .insert()
            .values(vec!["one()"])
            .build()
            .unwrap();
        let actual = storage.plan(input);
        let expected = Err(Error::Planner(PlannerError::InsertTableNotFound(
            "Missing".to_owned(),
        )));
        assert_eq!(actual, expected);
    }
}
