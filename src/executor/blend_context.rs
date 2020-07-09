use std::fmt::Debug;
use std::rc::Rc;

use sqlparser::ast::Ident;

use crate::data::Row;

#[derive(Debug)]
pub struct BlendContext<'a> {
    pub table_alias: &'a str,
    pub columns: Rc<Vec<Ident>>,
    pub row: Row,
    pub next: Option<Rc<BlendContext<'a>>>,
}
