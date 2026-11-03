use {
    super::{CustomFunction, Store},
    crate::{
        plan::StatementPlan,
        planner::{
            fetch_schema_map, plan_aggregate, plan_custom_function_calls, plan_hash_join,
            plan_primary_key, plan_schemaless, plan_table_columns, validate,
            validate_create_function,
        },
        result::Result,
    },
};

pub trait Planner: Store + CustomFunction {
    fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        let schema_map = fetch_schema_map(self, &statement)?;
        validate(&schema_map, &statement)?;

        let statement = plan_schemaless(&schema_map, statement)?;
        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_hash_join(&schema_map, statement);
        let statement = plan_aggregate(statement);
        validate_create_function(&statement)?;
        let statement = plan_custom_function_calls(self, statement)?;
        let statement = plan_table_columns(&schema_map, statement)?;

        Ok(statement)
    }
}
