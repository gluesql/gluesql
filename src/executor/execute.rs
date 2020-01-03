use crate::translator::{
    CommandQueue,
    CommandType::{
        GetSchema,
        SetSchema,
    },
};
use crate::storage::Store;

pub fn execute(storage: &dyn Store, queue: CommandQueue) -> bool {
    println!("execute! {}", queue.items.len());

    for command_type in queue.items {
        match command_type {
            GetSchema(table_name) => {
                let statement = storage.get_schema(table_name).unwrap();

                println!("get schema result is this {:#?}", statement);
            },
            SetSchema(statement) => {
                storage.set_schema(statement).unwrap();
            },
        }
    }

    true
}
