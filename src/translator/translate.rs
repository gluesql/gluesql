use crate::translator::{CommandQueue, CommandType, Filter};
use nom_sql::{
    DeleteStatement, SelectStatement,
    SqlQuery::{self, CreateTable, Delete, Insert, Select},
};

pub fn translate(sql_query: SqlQuery) -> CommandQueue {
    let items = match sql_query {
        Select(SelectStatement {
            tables,
            where_clause,
            ..
        }) => {
            println!("query type is SELECT");

            let table_name = tables
                .into_iter()
                .nth(0)
                .expect("SelectStatement->tables should have something")
                .name;
            let filter = Filter::from(where_clause);

            vec![CommandType::GetData(table_name, filter)]
        }
        Insert(statement) => {
            println!("query type is INSERT");

            vec![CommandType::SetData(statement)]
        }
        Delete(DeleteStatement {
            table,
            where_clause,
        }) => {
            println!("query type is DELETE");

            let table_name = table.name;
            let filter = Filter::from(where_clause);

            vec![CommandType::DelData(table_name, filter)]
        }
        CreateTable(statement) => {
            println!("query type is CREATE!!");

            vec![CommandType::SetSchema(statement)]
        }
        _ => {
            println!("not supported yet!");

            vec![]
        }
    };

    CommandQueue::from(items)
}
