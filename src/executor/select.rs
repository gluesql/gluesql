use crate::data::Row;
use crate::executor::{
    get_columns, Blend, BlendContext, BlendedFilter, Filter, FilterContext, Limit,
};
use crate::storage::Store;
use nom_sql::{JoinClause, JoinConstraint, JoinOperator, JoinRightSide, SelectStatement, Table};
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

pub fn join<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    join_clause: &'a JoinClause,
    filter_context: Option<&'a FilterContext<'a>>,
    blend_context: BlendContext<'a, T>,
) -> BlendContext<'a, T> {
    let JoinClause {
        operator,
        right,
        constraint,
    } = join_clause;

    match operator {
        JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin => (),
        _ => unimplemented!(),
    };

    let table = match right {
        JoinRightSide::Table(table) => table,
        _ => unimplemented!(),
    };
    let where_clause = match constraint {
        JoinConstraint::On(where_clause) => Some(where_clause),
        _ => unimplemented!(),
    };
    let filter = Filter::from((storage, where_clause, filter_context));
    let blended_filter = BlendedFilter::new(&filter, &blend_context);
    let columns = Rc::new(get_columns(storage, table));

    storage
        .get_data(&table.name)
        .unwrap()
        .map(move |(key, row)| (Rc::clone(&columns), key, row))
        .filter(move |(columns, _, row)| blended_filter.check(table, columns, row))
        .nth(0)
        .map(move |(columns, key, row)| BlendContext {
            table,
            columns,
            key,
            row,
            next: Some(Box::new(blend_context)),
        })
        .unwrap()
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
        join: join_clauses,
        fields,
        ..
    } = statement;
    let table = &tables
        .iter()
        .nth(0)
        .expect("SelectStatement->tables should have something");
    let blend = Blend::from(fields);
    let filter = Filter::from((storage, where_clause.as_ref(), filter_context));
    let limit = Limit::from(limit_clause);

    let rows = fetch_blended(storage, table, filter)
        .map(move |init_context| {
            join_clauses
                .iter()
                .fold(init_context, |blend_context, join_clause| {
                    join(storage, join_clause, filter_context, blend_context)
                })
        })
        .enumerate()
        .filter_map(move |(i, item)| match limit.check(i) {
            true => Some(item),
            false => None,
        })
        .map(move |BlendContext { columns, row, .. }| blend.apply(&columns, row));

    Box::new(rows)
}
