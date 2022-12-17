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

#[cfg(test)]
mod mock;

use crate::{ast::Statement, result::Result, store::Store};

use self::validate::update_schema_context;

pub use {
    self::validate::validate, error::*, index::plan as plan_index, join::plan as plan_join,
    primary_key::plan as plan_primary_key, schema::fetch_schema_map,
};

pub async fn plan(storage: &dyn Store, statement: Statement) -> Result<Statement> {
    let schema_map = fetch_schema_map(storage, &statement).await?;

    let schema_context = update_schema_context(&schema_map, &statement);

    // let statement = validate(&schema_map, statement)?;
    validate(schema_context, &statement)?;

    let statement = plan_primary_key(&schema_map, statement);
    let statement = plan_index(&schema_map, statement)?;
    let statement = plan_join(&schema_map, statement);

    Ok(statement)
}
