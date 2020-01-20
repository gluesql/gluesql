use crate::translator::Row;
use std::fmt::Debug;

pub enum Payload<T: Debug> {
    Create,
    Insert(Row<T>),
    Select(Box<dyn Iterator<Item = Row<T>>>),
    Delete(usize),
    Update(usize),
}
