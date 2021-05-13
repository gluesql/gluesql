use {
    super::BlendContext,
    crate::{ast::Function, data::Value},
    im_rc::HashMap,
    std::{fmt::Debug, rc::Rc},
};

#[derive(Debug)]
pub struct AggregateContext<'a> {
    pub aggregated: Option<HashMap<&'a Function, Value>>,
    pub next: Rc<BlendContext<'a>>,
}
