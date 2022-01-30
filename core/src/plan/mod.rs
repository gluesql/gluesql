mod index;
mod schema;

use {
    crate::{ast::Statement, result::Result, store::Store},
    std::fmt::Debug,
};

pub use {index::plan as plan_index, schema::fetch_schema_map};

pub async fn plan<T: Debug>(storage: &dyn Store<T>, statement: Statement) -> Result<Statement> {
    let schema_map = fetch_schema_map(storage, &statement).await?;

    plan_index(&schema_map, statement)
}
