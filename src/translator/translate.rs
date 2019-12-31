use nom_sql::SqlQuery;
use nom_sql::SqlQuery::{
    CreateTable,
    Insert,
    Select,
};
use crate::translator::CommandQueue;

pub fn translate(sql_query: SqlQuery) -> CommandQueue {
    match sql_query {
        Select(_) => { println!("query type is SELECT"); },
        Insert(_) => { println!("query type is INSERT"); },
        CreateTable(_) => { println!("query type is CREATE!!"); },
        _ => { println!("not supported yet!"); },
    }

    CommandQueue::new()
}
