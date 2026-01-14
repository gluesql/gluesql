use {
    super::RowContext,
    crate::{ast::Aggregate, data::Value},
    std::{collections::HashMap, fmt::Debug, sync::Arc},
};

#[derive(Debug)]
pub struct AggregateContext<'a> {
    pub aggregated: Option<HashMap<&'a Aggregate, Value>>,
    pub next: Arc<RowContext<'a>>,
}
