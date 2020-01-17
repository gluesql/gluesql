use crate::storage::Store;
use crate::translator::{CommandQueue, CommandType, Filter, Row};
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

pub fn execute<T: 'static>(storage: &dyn Store<T>, queue: CommandQueue) -> Result<(), ()>
where
    T: Debug,
{
    for command_type in queue.items {
        match command_type {
            CommandType::SetSchema(statement) => {
                storage.set_schema(statement).unwrap();
            }
            CommandType::GetData(table_name, filter) => {
                let rows = execute_get_data(storage, &table_name, filter).collect::<Vec<Row<T>>>();

                println!("GetData result-> \n{:#?}", rows);
            }
            CommandType::SetData(insert_statement) => {
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
            CommandType::DelData(table_name, filter) => {
                let rows = execute_get_data(storage, &table_name, filter);

                for row in rows {
                    storage.del_data(&table_name, &row.key).unwrap();
                }
            }
        }
    }

    Ok(())
}
