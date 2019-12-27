use crate::parser;
use parser::{QueryNode, Token};

use super::tokenize::tokenize;

pub fn parse(raw_sql: String) -> QueryNode {
    println!("raw sql: {}", raw_sql);

    let tokens = tokenize(raw_sql);

    println!("tokenized: \n\t{:?}\n", tokens);

    let query_type = match tokens[0] {
        Token::Query(query_type) => query_type,
        _ => panic!("Error handler is not implemented yet. :D"),
    };

    QueryNode::new(query_type)
}
