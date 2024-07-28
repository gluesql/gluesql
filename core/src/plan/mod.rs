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

    // First, we proceed to plan the primary key, i.e. we determine whether a primary
    // key is present in the WHERE statement and, if it is, we move it from the WHERE clause
    // to the index clause so that it can be used to optimize the query.
    let statement = plan_primary_key(&schema_map, statement);
    let statement = plan_index(&schema_map, statement)?;
    let statement = plan_join(&schema_map, statement);

    Ok(statement)
}
