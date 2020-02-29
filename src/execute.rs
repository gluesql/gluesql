use crate::data::Row;
use crate::executor::{fetch, select, Filter, Update};
use crate::storage::Store;
use nom_sql::{DeleteStatement, InsertStatement, SqlQuery, UpdateStatement};
use std::fmt::Debug;

pub enum Payload<T: Debug> {
    Create,
    Insert(Row<T>),
    Select(Vec<Row<T>>),
    Delete(usize),
    Update(usize),
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
            let rows = select(storage, statement, None).collect();

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
                table,
                where_clause,
            } = statement;
            let filter = Filter {
                storage,
                where_clause,
                context: None,
            };

            let num_rows = fetch(storage, table, filter).fold(0, |num, row| {
                storage.del_data(&table.name, &row.key).unwrap();

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
            let update = Update { fields };
            let filter = Filter {
                storage,
                where_clause,
                context: None,
            };

            let num_rows = fetch(storage, table, filter)
                .map(|row| update.apply(row))
                .fold(0, |num, row| {
                    storage.set_data(&table.name, row).unwrap();

                    num + 1
                });

            Payload::Update(num_rows)
        }
        _ => unimplemented!(),
    };

    Ok(payload)
}
