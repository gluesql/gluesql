use nom_sql::{Column, Table};
use std::fmt::Debug;
use std::rc::Rc;

use crate::data::Row;

#[derive(Debug)]
pub struct BlendContext<'a, T: 'static + Debug> {
    pub table: &'a Table,
    pub columns: &'a [Column],
    pub key: T,
    pub row: Row,
    pub next: Option<Rc<BlendContext<'a, T>>>,
}
