use crate::data::Row;
use crate::executor::Filter;
use crate::storage::Store;
use nom_sql::{Column, ColumnSpecification, Table};
use std::fmt::Debug;
use std::rc::Rc;

pub fn get_columns<T: 'static + Debug>(storage: &dyn Store<T>, table: &Table) -> Vec<Column> {
    storage
        .get_schema(&table.name)
        .unwrap()
        .fields
        .into_iter()
        .map(|ColumnSpecification { column, .. }| column)
        .collect::<Vec<Column>>()
}

pub fn fetch<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table: &'a Table,
    filter: Filter<'a, T>,
) -> Box<dyn Iterator<Item = (Rc<Vec<Column>>, T, Row)> + 'a> {
    let columns = Rc::new(get_columns(storage, table));

    let rows = storage
        .get_data(&table.name)
        .unwrap()
        .map(move |(key, row)| (Rc::clone(&columns), key, row))
        .filter(move |(columns, _, row)| filter.check(table, columns, row));

    Box::new(rows)
}
