use boolinator::Boolinator;
use futures::stream::{self, once, StreamExt, TryStream, TryStreamExt};
use serde::Serialize;
use std::fmt::Debug;
use std::pin::Pin;
use std::rc::Rc;
use thiserror::Error as ThisError;

use sqlparser::ast::{Ident, Join as AstJoin, JoinConstraint, JoinOperator};

use super::context::{BlendContext, FilterContext};
use super::filter::Filter;
use crate::data::Table;
use crate::result::{Error, Result};
use crate::store::Store;
use crate::utils::OrStream;

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
        let init_rows: Joined<'a> = Box::pin(stream::once(async { init_context }));
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
        table.get_alias(),
        Rc::clone(&columns),
        None,
        Some(Rc::clone(&blend_context)),
    ));

    let fetch_joined = |constraint: &'a JoinConstraint| {
        fetch_joined(
            storage,
            table_name,
            table_alias,
            Rc::clone(&columns),
            filter_context,
            blend_context,
            constraint,
        )
    };

    match join_operator {
        JoinOperator::Inner(constraint) => fetch_joined(constraint).map(|rows| {
            let rows: Joined<'a> = Box::pin(rows);

            rows
        }),
        JoinOperator::LeftOuter(constraint) => fetch_joined(constraint).map(|rows| {
            let init_rows = once(async { Ok(init_context) });
            let rows: Joined<'a> = Box::pin(OrStream::new(rows, init_rows));

            rows
        }),
        _ => Err(JoinError::JoinTypeNotSupported.into()),
    }
}

fn fetch_joined<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    table_name: &'a str,
    table_alias: &'a str,
    columns: Rc<Vec<Ident>>,
    filter_context: Option<Rc<FilterContext<'a>>>,
    blend_context: Rc<BlendContext<'a>>,
    constraint: &'a JoinConstraint,
) -> Result<impl TryStream<Ok = JoinItem<'a>, Error = Error, Item = Result<JoinItem<'a>>> + 'a> {
    let where_clause = match constraint {
        JoinConstraint::On(where_clause) => Some(where_clause),
        JoinConstraint::Using(_) => {
            return Err(JoinError::UsingOnJoinNotSupported.into());
        }
        JoinConstraint::Natural => {
            return Err(JoinError::NaturalOnJoinNotSupported.into());
        }
    };

    let rows = storage
        .scan_data(table_name)
        .map(stream::iter)?
        .try_filter_map(move |(_, row)| {
            let filter_context = FilterContext::concat(
                filter_context.as_ref().map(Rc::clone),
                Some(&blend_context).map(Rc::clone),
            );
            let filter_context = Some(filter_context).map(Rc::new);
            let blend_context = Rc::clone(&blend_context);
            let columns = Rc::clone(&columns);

            async move {
                let filter = Filter::new(storage, where_clause, filter_context, None);
                let context = Rc::new(BlendContext::new(
                    table_alias,
                    Rc::clone(&columns),
                    Some(row),
                    Some(&blend_context).map(Rc::clone),
                ));

                filter
                    .check(Rc::clone(&context))
                    .await
                    .map(|pass| pass.as_some(context))
            }
        });

    Ok(rows)
}
