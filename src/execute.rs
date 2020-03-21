use nom_sql::{DeleteStatement, InsertStatement, SqlQuery, UpdateStatement};
use std::fmt::Debug;

use crate::data::Row;
use crate::executor::{fetch, fetch_columns, fetch_join_columns, select, Filter, Update};
use crate::storage::Store;
use crate::*;

pub enum Payload {
    Create,
    Insert(Row),
    Select(Vec<Row>),
    Delete(usize),
    Update(usize),
}

pub fn execute<T: 'static + Debug>(
    storage: &dyn Store<T>,
    sql_query: &SqlQuery,
) -> Result<Payload> {
    let payload = match sql_query {
        SqlQuery::CreateTable(statement) => {
            storage.set_schema(statement)?;
            Payload::Create
        }
        SqlQuery::Select(statement) => {
            let (columns, join_columns) = fetch_join_columns(storage, statement);
            let rows = select(storage, statement, &columns, &join_columns, None).collect();

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
            let create_fields = storage.get_schema(table_name)?.fields;
            let key = storage.gen_id(table_name)?;
            let row = Row::new(create_fields, insert_fields, insert_data)?;

            let row = storage.set_data(&key, row)?;

            Payload::Insert(row)
        }
        SqlQuery::Delete(statement) => {
            let DeleteStatement {
                table,
                where_clause,
            } = statement;
            let filter = Filter::new(storage, where_clause.as_ref(), None);
            let columns = fetch_columns(storage, table);

            let num_rows = fetch(storage, table, &columns, filter).fold(0, |num, (_, key, _)| {
                storage.del_data(&key).unwrap();

                num + 1
            });

            Payload::Delete(num_rows)
        }
        SqlQuery::Update(statement) => {
            let UpdateStatement {
                table,
                fields,
                where_clause,
            } = statement;
            let update = Update::new(fields);
            let filter = Filter::new(storage, where_clause.as_ref(), None);
            let columns = fetch_columns(storage, table);

            let num_rows = fetch(storage, table, &columns, filter)
                .map(|(columns, key, row)| (key, update.apply(columns, row)))
                .fold(0, |num, (key, row)| {
                    storage.set_data(&key, row).unwrap();
                    num + 1
                });

            Payload::Update(num_rows)
        }
        _ => unimplemented!(),
    };

    Ok(payload)
}
