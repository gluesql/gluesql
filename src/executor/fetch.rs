use crate::executor::Filter;
use crate::data::Row;
use crate::storage::Store;
use nom_sql::Table;
use std::fmt::Debug;

pub fn fetch<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table: &'a Table,
    filter: Filter<'a, T>,
) -> Box<dyn Iterator<Item = Row<T>> + 'a> {
    let rows = storage
        .get_data(&table.name)
        .unwrap()
        .filter(move |row| filter.check(table, row));

    Box::new(rows)
}
