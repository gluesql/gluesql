use crate::data::Row;
use crate::executor::{get_columns, Blend, BlendContext, Filter, FilterContext, Limit};
use crate::storage::Store;
use nom_sql::{SelectStatement, Table};
use std::fmt::Debug;
use std::rc::Rc;

fn fetch_blended<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table: &'a Table,
    filter: Filter<'a, T>,
) -> Box<dyn Iterator<Item = BlendContext<'a, T>> + 'a> {
    let columns = Rc::new(get_columns(storage, table));

    let rows = storage
        .get_data(&table.name)
        .unwrap()
        .map(move |(key, row)| (Rc::clone(&columns), key, row))
        .filter(move |(columns, _, row)| filter.check(table, columns, row))
        .map(move |(columns, key, row)| BlendContext {
            table,
            columns,
            key,
            row,
            next: None,
        });

    Box::new(rows)
}

pub fn select<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    statement: &'a SelectStatement,
    filter_context: Option<&'a FilterContext<'a>>,
) -> Box<dyn Iterator<Item = Row> + 'a> {
    let SelectStatement {
        tables,
        where_clause,
        limit: limit_clause,
        fields,
        ..
    } = statement;
    let table = &tables
        .iter()
        .nth(0)
        .expect("SelectStatement->tables should have something");
    let blend = Blend::from(fields);
    let filter = Filter::from((storage, where_clause, filter_context));
    let limit = Limit::from(limit_clause);

    let rows = fetch_blended(storage, table, filter)
        .enumerate()
        .filter_map(move |(i, item)| match limit.check(i) {
            true => Some(item),
            false => None,
        })
        .map(move |BlendContext { columns, row, .. }| blend.apply(&columns, row));

    Box::new(rows)
}
