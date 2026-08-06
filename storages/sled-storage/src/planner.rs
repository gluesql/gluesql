use {
    crate::SledStorage,
    gluesql_core::{
        error::Result,
        plan::StatementPlan,
        planner::{
            fetch_schema_map, plan_aggregate, plan_hash_join, plan_index, plan_primary_key,
            plan_references, plan_schemaless, validate,
        },
        store::Planner,
    },
};

impl Planner for SledStorage {
    fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        let schema_map = fetch_schema_map(self, &statement)?;
        validate(&schema_map, &statement)?;

        let statement = plan_schemaless(&schema_map, statement)?;
        let statement = plan_references(&schema_map, statement)?;
        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_index(&schema_map, statement);
        let statement = plan_hash_join(&schema_map, statement);
        let statement = plan_aggregate(statement);

        Ok(statement)
    }
}
