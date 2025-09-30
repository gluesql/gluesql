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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
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

#[cfg(test)]
mod tests {
    use super::Vector;

    #[test]
    fn new_and_default_are_empty() {
        let vector = Vector::<i32>::new();
        assert!(vector.is_empty());
        assert_eq!(vector.len(), 0);

        let default_vector = Vector::<i32>::default();
        assert!(default_vector.is_empty());
        assert_eq!(default_vector.len(), 0);
    }

    #[test]
    fn push_update_remove_and_get() {
        let vector = Vector::new().push(1).push(2).push(3);
        assert_eq!(vector.len(), 3);
        assert_eq!(vector.get(0), Some(&1));
        assert_eq!(vector.get(2), Some(&3));

        let vector = vector.update(1, 10);
        assert_eq!(vector.get(1), Some(&10));

        let vector = vector.remove(0);
        assert_eq!(vector.len(), 2);
        assert_eq!(vector.get(0), Some(&10));
        assert_eq!(vector.get(1), Some(&3));
    }

    #[test]
    fn reverse_sort_and_sort_by() {
        let vector = Vector::new().push(3).push(1).push(2);
        let vector = vector.reverse();
        assert_eq!(vector.get(0), Some(&2));
        assert_eq!(vector.get(1), Some(&1));
        assert_eq!(vector.get(2), Some(&3));

        let vector = vector.sort();
        assert_eq!(vector.get(0), Some(&1));
        assert_eq!(vector.get(1), Some(&2));
        assert_eq!(vector.get(2), Some(&3));

        let vector = vector.sort_by(|a, b| b.cmp(a));
        assert_eq!(vector.get(0), Some(&3));
        assert_eq!(vector.get(1), Some(&2));
        assert_eq!(vector.get(2), Some(&1));
    }

    #[test]
    fn pop_returns_removed_value() {
        let vector = Vector::new().push(1).push(2);
        let (vector, popped) = vector.pop();
        assert_eq!(popped, Some(2));
        assert_eq!(vector.len(), 1);

        let (vector, popped) = vector.pop();
        assert_eq!(popped, Some(1));
        assert!(vector.is_empty());

        let (_, popped) = Vector::<i32>::new().pop();
        assert_eq!(popped, None);
    }

    #[test]
    fn conversions_cover_all_variants() {
        let base = vec![1, 2, 3];

        let vector_from_vec = Vector::from(base.clone());
        assert_eq!(vector_from_vec.len(), base.len());

        let vec_from_vector: Vec<_> = Vector::from(base.clone()).into();
        assert_eq!(vec_from_vector, base);

        let vector_from_iter: Vector<_> = base.clone().into_iter().collect();
        assert_eq!(vector_from_iter.len(), base.len());

        let vec_from_iter_vector: Vec<_> = vector_from_iter.into();
        assert_eq!(vec_from_iter_vector, base);

        let iterated: Vec<_> = Vector::from(vec![1, 2, 3]).into_iter().collect();
        assert_eq!(iterated, vec![1, 2, 3]);
    }
}
