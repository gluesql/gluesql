use nom_sql::SqlQuery;
use nom_sql::SqlQuery::{
    CreateTable,
    Insert,
    Select,
};
use crate::translator::{
    CommandType,
    CommandQueue,
};

pub fn translate(sql_query: SqlQuery) -> CommandQueue {
    let mut items: Vec<CommandType> = vec![];

    match sql_query {
        Select(_) => { println!("query type is SELECT"); },
        Insert(_) => { println!("query type is INSERT"); },
        CreateTable(statement) => {
            println!("query type is CREATE!!");

            let table_name = statement.table.name.clone();

            items.push(
                CommandType::SetSchema(statement)
            );

            // Test
            items.push(
                CommandType::GetSchema(table_name)
            );
        },
        _ => { println!("not supported yet!"); },
    }

    CommandQueue::from(items)
}
