use crate::executor::{fetch, Blend, Context, Filter, Limit};
use crate::data::Row;
use crate::storage::Store;
use nom_sql::SelectStatement;
use std::fmt::Debug;

pub fn select<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    statement: &'a SelectStatement,
    context: Option<&'a Context<'a, T>>,
) -> Box<dyn Iterator<Item = Row<T>> + 'a> {
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
    let blend = Blend { fields };
    let filter = Filter {
        storage,
        where_clause,
        context,
    };
    let limit = Limit { limit_clause };

    let rows = fetch(storage, table, filter)
        .enumerate()
        .filter(move |(i, _)| limit.check(i))
        .map(|(_, row)| row)
        .map(move |row| {
            let Row { key, items } = row;
            let items = items.into_iter().filter(|item| blend.check(item)).collect();

            Row { key, items }
        });

    Box::new(rows)
}
