use crate::translator::{CommandQueue, CommandType, Filter};
use nom_sql::SqlQuery;
use nom_sql::SqlQuery::{CreateTable, Delete, Insert, Select};

pub fn translate(sql_query: SqlQuery) -> CommandQueue {
    let items = match sql_query {
        Select(statement) => {
            println!("query type is SELECT");
            let table_name = statement.tables[0].name.clone();
            let filter = Filter::from(statement.where_clause);

            vec![CommandType::GetData(table_name, filter)]
        }
        Insert(statement) => {
            println!("query type is INSERT");

            vec![CommandType::SetData(statement)]
        }
        Delete(statement) => {
            println!("query type is DELETE");
            let table_name = statement.table.name.clone();
            let filter = Filter::from(statement.where_clause);

            vec![CommandType::DelData(table_name, filter)]
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
