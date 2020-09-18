use std::vec::IntoIter;

pub struct Vector<T>(Vec<T>);

impl<T> Vector<T> {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn push(mut self, value: T) -> Self {
        self.0.push(value);

        self
    }

    pub fn get(&self, i: usize) -> Option<&T> {
        self.0.get(i)
    }

    pub fn into_iter(self) -> IntoIter<T> {
        self.0.into_iter()
    }
}
