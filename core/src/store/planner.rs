use {
    super::Store,
    crate::{
        plan::{
            StatementPlan, fetch_schema_map, plan_aggregate, plan_join, plan_primary_key,
            plan_schemaless, validate,
        },
        result::Result,
    },
    async_trait::async_trait,
};

#[async_trait]
pub trait Planner: Store {
    async fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        let schema_map = fetch_schema_map(self, &statement).await?;
        validate(&schema_map, &statement)?;

        let statement = plan_schemaless(&schema_map, statement)?;
        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_join(&schema_map, statement);
        let statement = plan_aggregate(statement);

        Ok(statement)
    }
}
