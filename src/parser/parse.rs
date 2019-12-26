use rand::seq::IteratorRandom;

use crate::parser;
use parser::QueryType::{SELECT, INSERT, CREATE};
use parser::QueryNode;

pub fn parse(raw_sql: String) -> QueryNode {
    println!("raw sql is {}", raw_sql);

    let mut rng = rand::thread_rng();
    let query_types = [SELECT, INSERT, CREATE];
    let query_type = *query_types.iter().choose(&mut rng).unwrap();
    let query_node = QueryNode::new(query_type);

    query_node
}
