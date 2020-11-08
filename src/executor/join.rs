use boolinator::Boolinator;
use futures::stream::{self, StreamExt, TryStream, TryStreamExt};
use or_iterator::OrIterator;
use serde::Serialize;
use std::fmt::Debug;
use std::iter::once;
use std::pin::Pin;
use std::rc::Rc;
use thiserror::Error as ThisError;

use sqlparser::ast::{Ident, Join as AstJoin, JoinConstraint, JoinOperator};

use super::context::{BlendContext, FilterContext};
use super::filter::Filter;
use crate::data::Table;
use crate::result::{Error, Result};
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

type JoinItem<'a> = Rc<BlendContext<'a>>;
type Joined<'a> =
    Pin<Box<dyn TryStream<Ok = JoinItem<'a>, Error = Error, Item = Result<JoinItem<'a>>> + 'a>>;

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

    pub async fn apply(
        &self,
        init_context: Result<BlendContext<'a>>,
        join_columns: Rc<Vec<Rc<Vec<Ident>>>>,
    ) -> Result<Joined<'a>> {
        let init_context = init_context.map(Rc::new);
        let init_rows: Joined<'a> = Box::pin(stream::iter(once(init_context)));
        let filter_context = self.filter_context.as_ref().map(Rc::clone);

        let joins = self
            .join_clauses
            .iter()
            .enumerate()
            .map(move |(i, join_clause)| {
                let join_columns = Rc::clone(&join_columns[i]);

                Ok::<_, Error>((join_clause, join_columns))
            });

        let rows = stream::iter(joins)
            .try_fold(init_rows, |rows, (join_clause, join_columns)| {
                let storage = self.storage;
                let filter_context = filter_context.as_ref().map(Rc::clone);

                async move {
                    let rows = rows
                        .map(Ok)
                        .and_then(move |blend_context| {
                            let columns = Rc::clone(&join_columns);
                            let filter_context = filter_context.as_ref().map(Rc::clone);

                            join(storage, filter_context, join_clause, columns, blend_context)
                        })
                        .try_flatten();

                    Ok::<Joined<'a>, _>(Box::pin(rows))
                }
            })
            .await?;

        Ok(Box::pin(rows))
    }
}

async fn join<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: Option<Rc<FilterContext<'a>>>,
    ast_join: &'a AstJoin,
    columns: Rc<Vec<Ident>>,
    blend_context: Result<Rc<BlendContext<'a>>>,
) -> Result<Joined<'a>> {
    let AstJoin {
        relation,
        join_operator,
    } = ast_join;
    let table = Table::new(relation)?;
    let table_name = table.get_name();
    let table_alias = table.get_alias();

    let blend_context = blend_context?;
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
                .check(table_alias, Rc::clone(&columns), &row)
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
        JoinOperator::Inner(constraint) => fetch_rows(constraint).map(|rows| {
            let rows: Joined<'a> = Box::pin(stream::iter(rows));

            rows
        }),
        JoinOperator::LeftOuter(constraint) => fetch_rows(constraint).map(|rows| {
            let rows: Joined<'a> = Box::pin(stream::iter(rows.or(once(Ok(init_context)))));

            rows
        }),
        _ => Err(JoinError::JoinTypeNotSupported.into()),
    }
}
