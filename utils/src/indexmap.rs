use {
    indexmap::map::{IntoIter, Keys},
    std::{cmp::Eq, hash::Hash},
};

/// `HashMap` which provides
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

    pub fn keys(&self) -> Keys<'_, K, V> {
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

#[cfg(test)]
mod tests {
    use super::IndexMap;

    #[test]
    fn new_and_default_are_empty() {
        let map = IndexMap::<&str, i32>::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        let default_map = IndexMap::<&str, i32>::default();
        assert!(default_map.is_empty());
        assert_eq!(default_map.len(), 0);
    }

    #[test]
    fn insert_get_and_order() {
        let (map, previous) = IndexMap::new().insert("a", 1);
        assert!(previous.is_none());

        let (map, previous) = map.insert("b", 2);
        assert!(previous.is_none());

        let (map, previous) = map.insert("a", 3);
        assert_eq!(previous, Some(1));

        assert_eq!(map.len(), 2);
        assert!(!map.is_empty());
        assert_eq!(map.get(&"a"), Some(&3));
        assert_eq!(map.get(&"b"), Some(&2));

        let keys: Vec<_> = map.keys().copied().collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn into_iter_preserves_insertion_order() {
        let (map, _) = IndexMap::new().insert(1, "one");
        let (map, _) = map.insert(2, "two");
        let (map, _) = map.insert(1, "uno");

        let collected: Vec<_> = map.into_iter().collect();
        assert_eq!(collected, vec![(1, "uno"), (2, "two")]);
    }
}
