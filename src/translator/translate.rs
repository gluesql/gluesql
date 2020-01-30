use crate::translator::{Blend, CommandType, Filter, Limit, SelectTranslation, Update};
use nom_sql::{DeleteStatement, SelectStatement, SqlQuery, UpdateStatement};

pub fn translate_select<'a>(statement: &'a SelectStatement) -> SelectTranslation<'a> {
    let SelectStatement {
        tables,
        where_clause,
        limit,
        fields,
        ..
    } = statement;

    let table_name = &tables
        .iter()
        .nth(0)
        .expect("SelectStatement->tables should have something")
        .name;
    let blend = Blend::from(fields);
    let filter = Filter::from(where_clause);
    let limit = Limit::from(limit);

    SelectTranslation {
        table_name,
        blend,
        filter,
        limit,
    }
}

pub fn translate(sql_query: &SqlQuery) -> CommandType {
    match sql_query {
        SqlQuery::CreateTable(statement) => CommandType::Create(statement),
        SqlQuery::Insert(statement) => CommandType::Insert(statement),
        SqlQuery::Select(statement) => CommandType::Select(translate_select(statement)),
        SqlQuery::Delete(DeleteStatement {
            table,
            where_clause,
        }) => {
            let table_name = &table.name;
            let filter = Filter::from(where_clause);

            CommandType::Delete { table_name, filter }
        }
        SqlQuery::Update(UpdateStatement {
            table,
            fields,
            where_clause,
        }) => {
            let table_name = &table.name;
            let update = Update::from(fields);
            let filter = Filter::from(where_clause);

            CommandType::Update {
                table_name,
                update,
                filter,
            }
        }
        _ => {
            panic!("[translate.rs] query not supported");
        }
    }
}
