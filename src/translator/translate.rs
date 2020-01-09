use crate::translator::{CommandQueue, CommandType};
use nom_sql::SqlQuery;
use nom_sql::SqlQuery::{CreateTable, Insert, Select};

pub fn translate(sql_query: SqlQuery) -> CommandQueue {
    let items = match sql_query {
        Select(statement) => {
            println!("query type is SELECT");

            vec![CommandType::GetData(statement)]
        }
        Insert(statement) => {
            println!("query type is INSERT");

            vec![CommandType::SetData(statement)]
        }
        CreateTable(statement) => {
            println!("query type is CREATE!!");

            let table_name = statement.table.name.clone();

            vec![
                CommandType::SetSchema(statement),
                CommandType::GetSchema(table_name), // test
            ]
        }
        _ => {
            println!("not supported yet!");

            vec![]
        }
    };

    CommandQueue::from(items)
}
