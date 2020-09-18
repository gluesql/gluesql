use std::vec::IntoIter;

pub struct ImVector<T>(Vec<T>);

impl<T> ImVector<T> {
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
}

impl<T> IntoIterator for ImVector<T> {
    type Item = T;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
