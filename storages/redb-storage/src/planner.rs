use {
    crate::RedbStorage,
    gluesql_core::{
        error::Result,
        plan::StatementPlan,
        planner::{
            plan_aggregate, plan_create_table_columns, plan_custom_function_calls, plan_hash_join,
            plan_index, plan_primary_key, plan_schema_map, plan_schemaless, validate,
            validate_create_function, validate_create_index, validate_insert,
        },
        store::Planner,
    },
};

impl Planner for RedbStorage {
    fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        let statement = plan_schema_map(self, statement)?;

        validate(&statement)?;
        validate_create_index(&statement)?;

        let statement = plan_schemaless(statement)?;
        let statement = plan_primary_key(statement)?;
        let statement = plan_index(statement)?;
        let statement = plan_hash_join(statement)?;
        let statement = plan_aggregate(statement);

        validate_create_function(&statement)?;
        let statement = plan_custom_function_calls(self, statement)?;

        validate_insert(&statement)?;
        plan_create_table_columns(statement)
    }
}
