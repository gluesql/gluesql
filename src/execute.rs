use crate::row::Row;
use crate::storage::Store;
use crate::translator::{Blend, Filter, Limit, Update};
use nom_sql::{
    DeleteStatement, InsertStatement, SelectStatement, SqlQuery, Table, UpdateStatement,
};
use std::fmt::Debug;

pub enum Payload<T: Debug> {
    Create,
    Insert(Row<T>),
    Select(Vec<Row<T>>),
    Delete(usize),
    Update(usize),
}

fn execute_get_data<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table_name: &str,
    filter: Filter<'a>,
) -> Box<dyn Iterator<Item = Row<T>> + 'a> {
    let rows = storage
        .get_data(&table_name)
        .unwrap()
        .filter(move |row| filter.check(row));

    Box::new(rows)
}

pub fn execute<T: 'static + Debug>(
    storage: &dyn Store<T>,
    sql_query: &SqlQuery,
) -> Result<Payload<T>, ()> {
    let payload = match sql_query {
        SqlQuery::CreateTable(statement) => {
            storage.set_schema(statement).unwrap();

            Payload::Create
        }
        SqlQuery::Select(statement) => {
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

            let rows = execute_get_data(storage, &table_name, filter)
                .enumerate()
                .filter(move |(i, _)| limit.check(i))
                .map(|(_, row)| row)
                .map(move |row| {
                    let Row { key, items } = row;
                    let items = items.into_iter().filter(|item| blend.check(item)).collect();

                    Row { key, items }
                })
                .collect::<Vec<Row<T>>>();

            Payload::Select(rows)
        }
        SqlQuery::Insert(statement) => {
            let (table_name, insert_fields, insert_data) = match statement {
                InsertStatement {
                    table,
                    fields,
                    data,
                    ..
                } => (&table.name, fields, data),
            };
            let create_fields = storage.get_schema(table_name).unwrap().fields;
            let key = storage.gen_id().unwrap();
            let row = Row::from((key, create_fields, insert_fields, insert_data));

            let row = storage.set_data(table_name, row).unwrap();

            Payload::Insert(row)
        }
        SqlQuery::Delete(statement) => {
            let DeleteStatement {
                table: Table {
                    name: table_name, ..
                },
                where_clause,
            } = statement;
            let filter = Filter::from(where_clause);

            let num_rows = execute_get_data(storage, table_name, filter).fold(0, |num, row| {
                storage.del_data(table_name, &row.key).unwrap();

                num + 1
            });

            Payload::Delete(num_rows)
        }
        SqlQuery::Update(statement) => {
            let UpdateStatement {
                table: Table {
                    name: table_name, ..
                },
                fields,
                where_clause,
            } = statement;
            let update = Update::from(fields);
            let filter = Filter::from(where_clause);

            let num_rows = execute_get_data(storage, table_name, filter)
                .map(|row| update.apply(row))
                .fold(0, |num, row| {
                    storage.set_data(table_name, row).unwrap();

                    num + 1
                });

            Payload::Update(num_rows)
        }
        _ => unimplemented!(),
    };

    Ok(payload)
}
