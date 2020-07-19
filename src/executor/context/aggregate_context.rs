use im_rc::HashMap;
use std::fmt::Debug;
use std::rc::Rc;

use sqlparser::ast::Function;

use super::BlendContext;
use crate::data::Value;

#[derive(Debug)]
pub struct AggregateContext<'a> {
    pub aggregated: Option<HashMap<&'a Function, Value>>,
    pub next: Rc<BlendContext<'a>>,
}
