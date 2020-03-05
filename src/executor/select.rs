use crate::data::Row;
use crate::executor::{fetch, Blend, Context, Filter, Limit};
use crate::storage::Store;
use nom_sql::SelectStatement;
use std::fmt::Debug;

pub fn select<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    statement: &'a SelectStatement,
    context: Option<&'a Context<'a>>,
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
    let filter = Filter::from((storage, where_clause, context));
    let limit = Limit::from(limit_clause);

    let rows = fetch(storage, table, filter)
        .enumerate()
        .filter_map(move |(i, item)| match limit.check(i) {
            true => Some(item),
            false => None,
        })
        .map(move |(columns, _, row)| blend.apply(&columns, row));

    Box::new(rows)
}
