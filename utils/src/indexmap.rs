use {
    indexmap::map::{IntoIter, Keys},
    std::{cmp::Eq, hash::Hash},
};

/// HashMap which provides
/// 1. Immutable APIs
/// 2. Preserving insertion order
pub struct IndexMap<K, V>(indexmap::IndexMap<K, V>);

impl<K: Hash + Eq, V> IndexMap<K, V> {
    pub fn new() -> Self {
        Self(indexmap::IndexMap::new())
    }

    pub fn insert(mut self, key: K, value: V) -> (Self, Option<V>) {
        let existing = self.0.insert(key, value);

        (self, existing)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.0.get(key)
    }

    pub fn keys(&self) -> Keys<K, V> {
        self.0.keys()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<K: Hash + Eq, V> Default for IndexMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Hash + Eq, V> IntoIterator for IndexMap<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
