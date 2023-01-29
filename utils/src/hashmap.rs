use std::{collections::HashMap, hash::Hash};

pub trait HashMapExt<K, V, I> {
    fn concat(self, entries: I) -> Self;
}

impl<K, V, I> HashMapExt<K, V, I> for HashMap<K, V>
where
    K: Hash + Eq,
    I: Iterator<Item = (K, V)>,
{
    fn concat(mut self, entries: I) -> Self {
        for (key, value) in entries {
            self.insert(key, value);
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use {super::HashMapExt, std::collections::HashMap};

    #[test]
    fn concat() {
        let values: HashMap<&str, i64> = [("a", 10), ("b", 20)].into();
        let new_items = [("c", 30), ("d", 40), ("e", 50)];

        let actual = values.concat(new_items.into_iter());
        let expected = [("a", 10), ("b", 20), ("c", 30), ("d", 40), ("e", 50)].into();

        assert_eq!(actual, expected);
    }
}
