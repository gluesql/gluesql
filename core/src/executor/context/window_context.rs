use crate::data::Value;

/// Slot-indexed window function results for a single row.
///
/// Mirrors [`super::AggregateValues`], except one instance exists per row
/// (window functions are computed per row) rather than per group.
#[derive(Debug)]
pub struct WindowValues {
    values: Box<[Value]>,
}

impl WindowValues {
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            values: values.into_boxed_slice(),
        }
    }

    pub fn get(&self, slot: usize) -> Option<&Value> {
        self.values.get(slot)
    }
}
