use crate::parser::{
    QueryType::{SELECT, INSERT, CREATE},
    QueryNode,
};
use crate::translator::CommandQueue;

pub fn translate(query_node: QueryNode) -> CommandQueue {
    match query_node.query_type {
        SELECT => { println!("query type is SELECT"); },
        INSERT => { println!("query type is INSERT"); },
        CREATE => { println!("query type is CREATE!!"); },
    }

    CommandQueue::new()
}
