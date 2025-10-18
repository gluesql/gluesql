use {
    super::Store,
    crate::{
        ast::Statement,
        plan::{fetch_schema_map, plan_join, plan_primary_key, validate},
        result::Result,
    },
    async_trait::async_trait,
};

#[async_trait]
pub trait Planner: Store + Sized {
    async fn plan(&self, statement: Statement) -> Result<Statement> {
        let schema_map = fetch_schema_map(self, &statement).await?;
        validate(&schema_map, &statement)?;

        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_join(&schema_map, statement);

        Ok(statement)
    }
}
