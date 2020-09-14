use std::slice::{Chunks, Iter};

pub struct ImVector<T>(Vec<T>);

impl<T> ImVector<T> {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn push(mut self, value: T) -> Self {
        self.0.push(value);

        self
    }

    pub fn first(&self) -> Option<&T> {
        self.0.first()
    }

    pub fn iter(&self) -> Iter<T> {
        self.0.iter()
    }

    pub fn chunks(&self, chunk_size: usize) -> Chunks<T> {
        self.0.chunks(chunk_size)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}
