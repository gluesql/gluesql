use crate::translator::CommandType;
use nom_sql::SqlQuery;

pub fn translate(sql_query: &SqlQuery) -> CommandType {
    match sql_query {
        SqlQuery::CreateTable(statement) => CommandType::Create(statement),
        SqlQuery::Insert(statement) => CommandType::Insert(statement),
        SqlQuery::Select(statement) => CommandType::Select(statement),
        SqlQuery::Delete(statement) => CommandType::Delete(statement),
        SqlQuery::Update(statement) => CommandType::Update(statement),
        _ => {
            panic!("[translate.rs] query not supported");
        }
    }
}
