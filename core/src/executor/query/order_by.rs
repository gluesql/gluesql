pub(super) mod select;
pub(super) mod values;

pub use select::SortError;

use {crate::data::Key, std::cmp::Ordering};

pub(super) fn sort_by(keys_a: &[(Key, Option<bool>)], keys_b: &[(Key, Option<bool>)]) -> Ordering {
    let pairs = keys_a
        .iter()
        .map(|(a, _)| a)
        .zip(keys_b.iter())
        .map(|(a, (b, asc))| (a, b, asc.unwrap_or(true)));

    for (key_a, key_b, asc) in pairs {
        match (key_a.cmp(key_b), asc) {
            (Ordering::Equal, _) => {}
            (ord, true) => return ord,
            (ord, false) => return ord.reverse(),
        }
    }

    Ordering::Equal
}
