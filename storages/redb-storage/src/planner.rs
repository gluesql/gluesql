use {
    crate::RedbStorage,
    gluesql_core::{
        error::Result,
        plan::{
            StatementPlan, fetch_schema_map, plan_aggregate, plan_index, plan_join,
            plan_primary_key, plan_schemaless, validate,
        },
        store::Planner,
    },
};

impl Planner for RedbStorage {
    fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        let schema_map = fetch_schema_map(self, &statement)?;
        validate(&schema_map, &statement)?;

        let statement = plan_schemaless(&schema_map, statement)?;
        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_index(&schema_map, statement);
        let statement = plan_join(&schema_map, statement);
        let statement = plan_aggregate(statement);

        Ok(statement)
    }
}
