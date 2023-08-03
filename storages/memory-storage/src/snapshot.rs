use {
    serde::{Deserialize, Serialize},
    std::fmt::Debug,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot<T> {
    pub data: T,
}

impl<T> Snapshot<T> {
    pub fn new(prev: T) -> Self {
        Self { data: prev }
    }
}
