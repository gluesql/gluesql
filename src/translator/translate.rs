use crate::translator::{CommandQueue, CommandType, Filter};
use nom_sql::SqlQuery;
use nom_sql::SqlQuery::{CreateTable, Insert, Select};

pub fn translate(sql_query: SqlQuery) -> CommandQueue {
    let items = match sql_query {
        Select(statement) => {
            println!("query type is SELECT");
            let table_name = statement.tables[0].name.clone();
            let filter = Filter::from(statement);

            vec![CommandType::GetData(table_name, filter)]
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
