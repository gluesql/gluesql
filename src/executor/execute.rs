use crate::storage::Store;
use crate::translator::{
    CommandQueue,
    CommandType::{GetData, GetSchema, SetData, SetSchema},
};

use crate::executor::Row;

pub fn execute(storage: &dyn Store, queue: CommandQueue) -> Result<(), ()> {
    for command_type in queue.items {
        match command_type {
            GetSchema(table_name) => {
                let statement = storage.get_schema(&table_name).unwrap();

                println!("GetSchema result -> \n{:#?}", statement);
            }
            SetSchema(statement) => {
                storage.set_schema(statement).unwrap();
            }
            GetData(table_name) => {
                let result_set = storage.get_data(&table_name).unwrap();

                let rows = result_set.collect::<Vec<Row>>();
                println!("GetData result-> \n{:#?}", rows);
            }
            SetData(insert_statement) => {
                let table_name = insert_statement.table.name.clone();
                let create_statement = storage.get_schema(&table_name).unwrap();

                let row = Row::from((create_statement, insert_statement));

                storage.set_data(&table_name, row).unwrap();
            }
        }
    }

    Ok(())
}
