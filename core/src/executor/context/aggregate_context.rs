use {
    super::RowContext,
    crate::{
        ast::Aggregate,
        data::Value,
        shared::{HashMap, Rc},
    },
    std::fmt::Debug,
};

#[derive(Debug)]
pub struct AggregateContext<'a> {
    pub aggregated: Option<HashMap<&'a Aggregate, Value>>,
    pub next: Rc<RowContext<'a>>,
}
