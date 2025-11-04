use {
    crate::SledStorage,
    async_trait::async_trait,
    gluesql_core::{
        ast::Statement,
        error::Result,
        plan::{fetch_schema_map, plan_index, plan_join, plan_primary_key, validate},
        store::Planner,
    },
};

#[async_trait]
impl Planner for SledStorage {
    async fn plan(&self, statement: Statement) -> Result<Statement> {
        let schema_map = fetch_schema_map(self, &statement).await?;
        validate(&schema_map, &statement)?;

        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_index(&schema_map, statement);
        let statement = plan_join(&schema_map, statement);

        Ok(statement)
    }
}
