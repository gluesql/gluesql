use std::{convert::From, vec::IntoIter};

pub struct Vector<T>(Vec<T>);

impl<T> Vector<T> {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn push(mut self, value: T) -> Self {
        self.0.push(value);

        self
    }

    pub fn update(mut self, i: usize, value: T) -> Self {
        self.0[i] = value;

        self
    }

    pub fn remove(mut self, i: usize) -> Self {
        self.0.remove(i);

        self
    }

    pub fn reverse(mut self) -> Self {
        self.0.reverse();

        self
    }

    pub fn sort(mut self) -> Self
    where
        T: std::cmp::Ord,
    {
        self.0.sort();

        self
    }

    pub fn sort_by<F>(mut self, compare: F) -> Self
    where
        F: FnMut(&T, &T) -> std::cmp::Ordering,
    {
        self.0.sort_by(compare);

        self
    }

    pub fn pop(mut self) -> (Self, Option<T>) {
        let v = self.0.pop();

        (self, v)
    }

    pub fn get(&self, i: usize) -> Option<&T> {
        self.0.get(i)
    }
}

impl<T> Default for Vector<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IntoIterator for Vector<T> {
    type Item = T;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> FromIterator<T> for Vector<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self(iter.into_iter().collect())
    }
}

impl<T> From<Vec<T>> for Vector<T> {
    fn from(vector: Vec<T>) -> Self {
        Self(vector)
    }
}

impl<T> From<Vector<T>> for Vec<T> {
    fn from(vector: Vector<T>) -> Self {
        vector.0
    }
}
