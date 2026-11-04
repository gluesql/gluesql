use {
    crate::SledStorage,
    gluesql_core::{
        error::Result,
        plan::StatementPlan,
        planner::{
            fetch_schema_map, plan_aggregate, plan_create_table_columns,
            plan_custom_function_calls, plan_hash_join, plan_index, plan_insert_table_columns,
            plan_primary_key, plan_schemaless, validate, validate_create_function,
            validate_create_index,
        },
        store::Planner,
    },
};

impl Planner for SledStorage {
    fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        let schema_map = fetch_schema_map(self, &statement)?;
        validate(&schema_map, &statement)?;
        validate_create_index(&schema_map, &statement)?;

        let statement = plan_schemaless(&schema_map, statement)?;
        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_index(&schema_map, statement);
        let statement = plan_hash_join(&schema_map, statement);
        let statement = plan_aggregate(statement);
        validate_create_function(&statement)?;
        let statement = plan_custom_function_calls(self, statement)?;
        let statement = plan_insert_table_columns(&schema_map, statement)?;
        let statement = plan_create_table_columns(&schema_map, statement)?;

        Ok(statement)
    }
}
