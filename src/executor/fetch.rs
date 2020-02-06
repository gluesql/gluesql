use crate::executor::Filter;
use crate::row::Row;
use crate::storage::Store;
use std::fmt::Debug;

pub fn fetch<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table_name: &str,
    filter: Filter<'a, T>,
) -> Box<dyn Iterator<Item = Row<T>> + 'a> {
    let rows = storage
        .get_data(&table_name)
        .unwrap()
        .filter(move |row| filter.check(row));

    Box::new(rows)
}
