use boolinator::Boolinator;
use iter_enum::Iterator;
use or_iterator::OrIterator;
use serde::Serialize;
use std::fmt::Debug;
use std::iter::once;
use std::rc::Rc;
use thiserror::Error as ThisError;

use sqlparser::ast::{Ident, Join as AstJoin, JoinConstraint, JoinOperator};

use super::context::{BlendContext, FilterContext};
use super::filter::Filter;
use crate::data::Table;
use crate::result::Result;
use crate::store::Store;

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum JoinError {
    #[error("unimplemented! join not supported")]
    JoinTypeNotSupported,

    #[error("unimplemented! using on join not supported")]
    UsingOnJoinNotSupported,

    #[error("unimplemented! natural on join not supported")]
    NaturalOnJoinNotSupported,

    #[error("umimplemented! failed to get table name")]
    FailedToGetTableName,
}

pub struct Join<'a, T: 'static + Debug> {
    storage: &'a dyn Store<T>,
    join_clauses: &'a [AstJoin],
    filter_context: Option<Rc<FilterContext<'a>>>,
}

type JoinItem<'a> = Result<Rc<BlendContext<'a>>>;

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
        join_clauses: &'a [AstJoin],
        filter_context: Option<Rc<FilterContext<'a>>>,
    ) -> Self {
        Self {
            storage,
            join_clauses,
            filter_context,
        }
    }

    pub fn apply(
        &self,
        init_context: Result<BlendContext<'a>>,
        join_columns: Rc<Vec<Rc<Vec<Ident>>>>,
    ) -> impl Iterator<Item = JoinItem<'a>> + 'a {
        let init_context = init_context.map(Rc::new);
        let init_rows = Applied::Init(once(init_context));
        let filter_context = self.filter_context.as_ref().map(Rc::clone);

        self.join_clauses
            .iter()
            .enumerate()
            .map(move |(i, join_clause)| {
                let join_columns = Rc::clone(&join_columns[i]);

                (join_clause, join_columns)
            })
            .fold(init_rows, |rows, (join_clause, join_columns)| {
                let storage = self.storage;
                let filter_context = filter_context.as_ref().map(Rc::clone);
                let map = move |blend_context| {
                    let columns = Rc::clone(&join_columns);

                    let filter_context = filter_context.as_ref().map(Rc::clone);
                    join(storage, filter_context, join_clause, columns, blend_context)
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
            })
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
    filter_context: Option<Rc<FilterContext<'a>>>,
    ast_join: &'a AstJoin,
    columns: Rc<Vec<Ident>>,
    blend_context: Result<Rc<BlendContext<'a>>>,
) -> impl Iterator<Item = JoinItem<'a>> + 'a {
    let err = |e| Joined::Err(once(Err(e)));

    macro_rules! try_into {
        ($v: expr) => {
            match $v {
                Ok(v) => v,
                Err(e) => {
                    return err(e);
                }
            }
        };
    }

    let AstJoin {
        relation,
        join_operator,
    } = ast_join;
    let table = try_into!(Table::new(relation));
    let table_name = table.get_name();
    let table_alias = table.get_alias();

    let blend_context = try_into!(blend_context);
    let init_context = Rc::new(BlendContext::new(
        table_alias,
        Rc::clone(&columns),
        None,
        Some(Rc::clone(&blend_context)),
    ));

    let fetch_rows = |constraint: &'a JoinConstraint| {
        let where_clause = match constraint {
            JoinConstraint::On(where_clause) => Some(where_clause),
            JoinConstraint::Using(_) => {
                return Err(JoinError::UsingOnJoinNotSupported.into());
            }
            JoinConstraint::Natural => {
                return Err(JoinError::NaturalOnJoinNotSupported.into());
            }
        };

        let rows = storage.scan_data(table_name)?;
        let rows = rows.filter_map(move |item| {
            let (_, row) = match item {
                Ok(v) => v,
                Err(e) => {
                    return Some(Err(e));
                }
            };

            let filter_context = filter_context.as_ref().map(Rc::clone);
            let filter_context = blend_context.concat_into(filter_context);
            let filter = Filter::new(storage, where_clause, filter_context, None);

            filter
                .check(table_alias, &columns, &row)
                .map(|pass| {
                    pass.as_some(Rc::new(BlendContext::new(
                        table_alias,
                        Rc::clone(&columns),
                        Some(row),
                        Some(Rc::clone(&blend_context)),
                    )))
                })
                .transpose()
        });

        Ok(rows)
    };

    match join_operator {
        JoinOperator::Inner(constraint) => match fetch_rows(constraint) {
            Ok(rows) => Joined::Inner(rows),
            Err(e) => err(e),
        },
        JoinOperator::LeftOuter(constraint) => match fetch_rows(constraint) {
            Ok(rows) => {
                let rows = rows.or(once(Ok(init_context)));

                Joined::LeftOuter(rows)
            }
            Err(e) => err(e),
        },
        _ => err(JoinError::JoinTypeNotSupported.into()),
    }
}
