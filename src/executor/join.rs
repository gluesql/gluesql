use boolinator::Boolinator;
use either::Either::{Left as L, Right as R};
use nom_sql::{Column, JoinClause, JoinConstraint, JoinOperator, Table};
use std::fmt::Debug;
use std::iter::once;
use std::rc::Rc;
use thiserror::Error as ThisError;

use crate::executor::{BlendContext, BlendedFilter, Filter, FilterContext};
use crate::result::Result;
use crate::storage::Store;

#[derive(ThisError, Debug, PartialEq)]
pub enum JoinError {
    #[error("unimplemented! join not supported")]
    JoinTypeNotSupported,

    #[error("unimplemented! using on join not supported")]
    UsingOnJoinNotSupported,
}

pub struct Join<'a, T: 'static + Debug> {
    storage: &'a dyn Store<T>,
    join_clauses: &'a Vec<JoinClause>,
    join_columns: &'a Vec<(&'a Table, Vec<Column>)>,
    filter_context: Option<&'a FilterContext<'a>>,
}

type JoinItem<'a, T> = Result<Rc<BlendContext<'a, T>>>;

macro_rules! try_some {
    ($expr: expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                return Some(Err(e));
            }
        }
    };
}

impl<'a, T: 'static + Debug> Join<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        join_clauses: &'a Vec<JoinClause>,
        join_columns: &'a Vec<(&'a Table, Vec<Column>)>,
        filter_context: Option<&'a FilterContext<'a>>,
    ) -> Self {
        Self {
            storage,
            join_clauses,
            join_columns,
            filter_context,
        }
    }

    pub fn apply(
        &self,
        init_context: Result<BlendContext<'a, T>>,
    ) -> Box<dyn Iterator<Item = JoinItem<'a, T>> + 'a> {
        let init_context = init_context.map(|c| Rc::new(c));
        let init_rows = once(init_context);

        self.join_clauses.iter().zip(self.join_columns.iter()).fold(
            Box::new(init_rows),
            |rows, (join_clause, (table, columns))| {
                let storage = self.storage;
                let filter_context = self.filter_context;

                Box::new(rows.flat_map(move |blend_context| {
                    join(
                        storage,
                        filter_context,
                        join_clause,
                        table,
                        columns,
                        blend_context,
                    )
                }))
            },
        )
    }
}

fn join<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: Option<&'a FilterContext<'a>>,
    join_clause: &'a JoinClause,
    table: &'a Table,
    columns: &'a Vec<Column>,
    blend_context: Result<Rc<BlendContext<'a, T>>>,
) -> impl Iterator<Item = JoinItem<'a, T>> + 'a {
    macro_rules! try_iter {
        ($expr: expr) => {
            match $expr {
                Ok(v) => v,
                Err(e) => {
                    return L(once(Err(e)));
                }
            }
        };
    }

    let blend_context = try_iter!(blend_context);

    let JoinClause {
        operator,
        constraint,
        ..
    } = join_clause;

    let where_clause = match constraint {
        JoinConstraint::On(where_clause) => Some(where_clause),
        JoinConstraint::Using(_) => {
            return L(once(Err(JoinError::UsingOnJoinNotSupported.into())));
        }
    };

    let init_context = Rc::clone(&blend_context);
    let rows = try_iter!(storage.get_data(&table.name));
    let rows = rows.filter_map(move |item| {
        let (key, row) = try_some!(item);

        let filter = Filter::new(storage, where_clause, filter_context);
        let blended_filter = BlendedFilter::new(&filter, Some(&blend_context));

        blended_filter
            .check(table, columns, &row)
            .map(|pass| {
                pass.as_some(Rc::new(BlendContext {
                    table,
                    columns,
                    key,
                    row,
                    next: Some(Rc::clone(&blend_context)),
                }))
            })
            .transpose()
    });

    match operator {
        JoinOperator::Join | JoinOperator::InnerJoin => R(L(rows)),
        JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin => R(R(rows
            .map(|row| {
                let is_last = false;
                let item = (is_last, row?);

                Ok(item)
            })
            .chain({
                let is_last = true;
                let item = (is_last, init_context);

                once(Ok(item))
            })
            .enumerate()
            .filter_map(|(i, item)| {
                let (is_last, blend_context) = try_some!(item);

                match !is_last || i == 0 {
                    true => Some(Ok(blend_context)),
                    false => None,
                }
            }))),
        _ => L(once(Err(JoinError::JoinTypeNotSupported.into()))),
    }
}
