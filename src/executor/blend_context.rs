use std::fmt::Debug;
use std::rc::Rc;

use sqlparser::ast::Ident;

use crate::data::Row;

#[derive(Debug)]
pub struct BlendContext<'a, T: 'static + Debug> {
    pub table_alias: &'a str,
    pub columns: Rc<Vec<Ident>>,
    pub key: T,
    pub row: Row,
    pub next: Option<Rc<BlendContext<'a, T>>>,
}
