use {
    crate::{ast::Statement, data::Schema, result::Result},
    std::collections::HashMap,
};

pub fn validate(schema_map: &HashMap<String, Schema>, statement: Statement) -> Result<Statement> {
    Ok(statement)
}
