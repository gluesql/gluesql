mod context;
mod evaluable;
mod expr;
mod index;
mod join;
mod schema;

#[cfg(test)]
mod mock;

use crate::{ast::Statement, result::Result, store::Store};

pub use {index::plan as plan_index, join::plan as plan_join, schema::fetch_schema_map};

pub async fn plan(storage: &dyn Store, statement: Statement) -> Result<Statement> {
    let schema_map = fetch_schema_map(storage, &statement).await?;

    let statement = plan_index(&schema_map, statement)?;
    let statement = plan_join(&schema_map, statement);

    Ok(statement)
}
