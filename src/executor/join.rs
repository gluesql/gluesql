use boolinator::Boolinator;
use iter_enum::Iterator;
use nom_sql::{Column, JoinClause, JoinConstraint, JoinOperator, Table};
use or_iterator::OrIterator;
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

#[derive(Iterator)]
enum Applied<I1, I2, I3, I4> {
    Init(I1),
    Once(I2),
    Boxed(I3),
    Mapped(I4),
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
    ) -> impl Iterator<Item = JoinItem<'a, T>> + 'a {
        let init_context = init_context.map(Rc::new);
        let init_rows = Applied::Init(once(init_context));

        self.join_clauses.iter().zip(self.join_columns.iter()).fold(
            init_rows,
            |rows, (join_clause, (table, columns))| {
                let storage = self.storage;
                let filter_context = self.filter_context;

                let map = move |blend_context| {
                    join(
                        storage,
                        filter_context,
                        join_clause,
                        table,
                        columns,
                        blend_context,
                    )
                };

                match rows {
                    Applied::Init(rows) => Applied::Once(rows.flat_map(map)),
                    Applied::Once(rows) => {
                        let rows: Box<dyn Iterator<Item = _>> = Box::new(rows.flat_map(map));

                        Applied::Boxed(rows)
                    }
                    Applied::Boxed(rows) => Applied::Mapped(rows.flat_map(map)),
                    Applied::Mapped(rows) => Applied::Boxed(Box::new(rows.flat_map(map))),
                }
            },
        )
    }
}

#[derive(Iterator)]
enum Joined<I1, I2, I3> {
    Err(I1),
    Inner(I2),
    LeftOuter(I3),
}

fn join<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: Option<&'a FilterContext<'a>>,
    join_clause: &'a JoinClause,
    table: &'a Table,
    columns: &'a Vec<Column>,
    blend_context: Result<Rc<BlendContext<'a, T>>>,
) -> impl Iterator<Item = JoinItem<'a, T>> + 'a {
    let err = |e| Joined::Err(once(Err(e)));

    macro_rules! try_iter {
        ($expr: expr) => {
            match $expr {
                Ok(v) => v,
                Err(e) => {
                    return err(e);
                }
            }
        };
    }

    let JoinClause {
        operator,
        constraint,
        ..
    } = join_clause;

    let where_clause = match constraint {
        JoinConstraint::On(where_clause) => Some(where_clause),
        JoinConstraint::Using(_) => {
            return err(JoinError::UsingOnJoinNotSupported.into());
        }
    };

    let blend_context = try_iter!(blend_context);
    let init_context = Rc::clone(&blend_context);
    let rows = try_iter!(storage.get_data(&table.name));
    let rows = rows.filter_map(move |item| {
        let (key, row) = match item {
            Ok(v) => v,
            Err(e) => {
                return Some(Err(e));
            }
        };

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
        JoinOperator::Join | JoinOperator::InnerJoin => Joined::Inner(rows),
        JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin => {
            let rows = rows.or(once(Ok(init_context)));

            Joined::LeftOuter(rows)
        }
        _ => err(JoinError::JoinTypeNotSupported.into()),
    }
}
