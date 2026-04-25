use {super::RowContext, crate::data::Value, std::sync::Arc};

#[derive(Debug)]
pub struct AggregateValues {
    values: Box<[Value]>,
}

impl AggregateValues {
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            values: values.into_boxed_slice(),
        }
    }

    pub fn get(&self, slot: usize) -> Option<&Value> {
        self.values.get(slot)
    }
}

#[derive(Debug)]
pub struct AggregateContext<'a> {
    pub aggregated: Option<Arc<AggregateValues>>,
    pub next: Option<Arc<RowContext<'a>>>,
}
