use crate::parser;
use parser::QueryType;

pub struct QueryNode {
    pub query_type: QueryType,
}

impl QueryNode{
    pub fn new(query_type: QueryType) -> QueryNode {
        QueryNode { query_type }
    }
}
