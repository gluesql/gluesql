use crate::storage::Store;
use crate::translator::{CommandType, Filter, Row};
use nom_sql::InsertStatement;
use std::fmt::Debug;

fn execute_get_data<T: 'static>(
    storage: &dyn Store<T>,
    table_name: &str,
    filter: Filter,
) -> Box<dyn Iterator<Item = Row<T>>>
where
    T: Debug,
{
    let rows = storage
        .get_data(&table_name)
        .unwrap()
        .filter(move |row| filter.check(row));

    Box::new(rows)
}

pub fn execute<T: 'static>(storage: &dyn Store<T>, command_type: CommandType) -> Result<(), ()>
where
    T: Debug,
{
    match command_type {
        CommandType::Create(statement) => {
            storage.set_schema(statement).unwrap();
        }
        CommandType::Select(table_name, filter) => {
            let rows = execute_get_data(storage, &table_name, filter).collect::<Vec<Row<T>>>();

            println!("SELECT result-> \n{:#?}", rows);
        }
        CommandType::Insert(insert_statement) => {
            let (table_name, insert_fields, insert_data) = match insert_statement {
                InsertStatement {
                    table,
                    fields,
                    data,
                    ..
                } => (table.name, fields, data),
            };
            let create_fields = storage.get_schema(&table_name).unwrap().fields;
            let key = storage.gen_id().unwrap();
            let row = Row::from((key, create_fields, insert_fields, insert_data));

            storage.set_data(&table_name, row).unwrap();
        }
        CommandType::Delete(table_name, filter) => {
            let rows = execute_get_data(storage, &table_name, filter);

            for row in rows {
                storage.del_data(&table_name, &row.key).unwrap();
            }
        }
        CommandType::Update(table_name, update, filter) => {
            let rows = execute_get_data(storage, &table_name, filter).map(|row| update.apply(row));

            for row in rows {
                storage.set_data(&table_name, row).unwrap();
            }
        }
    }

    Ok(())
}
