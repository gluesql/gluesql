use crate::translator::{
    CommandQueue,
    CommandType::{GET, SET},
};
use crate::storage::Store;

pub fn execute(storage: &dyn Store, queue: CommandQueue) -> bool {
    println!("execute! {}", queue.items.len());

    for command_type in queue.items {
        match command_type {
            GET => storage.get(),
            SET => storage.set(),
        }
    }

    true
}
