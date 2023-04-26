mod context;
mod error;
mod evaluable;
mod expr;
mod index;
mod join;
mod planner;
mod primary_key;
mod schema;
mod validate;

use crate::{ast::Statement, result::Result, store::Store};

pub use {
    self::validate::validate, error::*, index::plan as plan_index, join::plan as plan_join,
    primary_key::plan as plan_primary_key, schema::fetch_schema_map,
};

pub async fn plan<T: Store>(storage: &T, statement: Statement) -> Result<Statement> {
    let schema_map = fetch_schema_map(storage, &statement).await?;
    validate(&schema_map, &statement)?;
    let statement = plan_primary_key(&schema_map, statement);
    let statement = plan_index(&schema_map, statement)?;
    let statement = plan_join(&schema_map, statement);

    Ok(statement)
}
