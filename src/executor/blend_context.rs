use crate::data::Row;
use nom_sql::{Column, Table};
use std::fmt::Debug;
use std::rc::Rc;

#[derive(Debug)]
pub struct BlendContext<'a, T: Debug + 'static> {
    pub table: &'a Table,
    pub columns: Rc<Vec<Column>>,
    pub key: T,
    pub row: Row,
    pub next: Option<Box<BlendContext<'a, T>>>,
}
