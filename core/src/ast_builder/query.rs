use crate::{
    ast::Query,
    parse_sql::parse_query,
    result::{Error, Result},
    translate::translate_query,
};

#[derive(Clone)]
pub enum QueryNode {
    Text(String),
}

impl From<&str> for QueryNode {
    fn from(query: &str) -> Self {
        Self::Text(query.to_owned())
    }
}

impl TryFrom<QueryNode> for Query {
    type Error = Error;

    fn try_from(query_node: QueryNode) -> Result<Self> {
        match query_node {
            QueryNode::Text(query_node) => {
                parse_query(query_node).and_then(|item| translate_query(&item))
            }
        }
    }
}
