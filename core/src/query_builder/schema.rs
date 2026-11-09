use crate::{
    data::Schema,
    result::{Error, Result},
};

#[derive(Clone, Debug)]
pub enum SchemaNode {
    Text(String),
    Schema(Schema),
}

impl From<&str> for SchemaNode {
    fn from(ddl: &str) -> Self {
        Self::Text(ddl.to_owned())
    }
}

impl From<String> for SchemaNode {
    fn from(ddl: String) -> Self {
        Self::Text(ddl)
    }
}

impl From<Schema> for SchemaNode {
    fn from(schema: Schema) -> Self {
        Self::Schema(schema)
    }
}

impl TryFrom<SchemaNode> for Schema {
    type Error = Error;

    fn try_from(node: SchemaNode) -> Result<Self> {
        match node {
            SchemaNode::Text(ddl) => Self::from_ddl(&ddl),
            SchemaNode::Schema(schema) => Ok(schema),
        }
    }
}
