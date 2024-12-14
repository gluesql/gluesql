use {
    super::RowContext,
    crate::{ast::Aggregate, data::Value, Grc, HashMap},
    std::fmt::Debug,
};

#[derive(Debug)]
pub struct AggregateContext<'a> {
    pub aggregated: Option<HashMap<&'a Aggregate, Value>>,
    pub next: Grc<RowContext<'a>>,
}
