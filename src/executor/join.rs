use boolinator::Boolinator;
use nom_sql::{Column, JoinClause, JoinConstraint, JoinOperator, Table};
use std::fmt::Debug;
use thiserror::Error;

use crate::executor::{BlendContext, BlendedFilter, Filter, FilterContext};
use crate::result::Result;
use crate::storage::Store;

#[derive(Error, Debug, PartialEq)]
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
    ) -> Option<Result<BlendContext<'a, T>>> {
        let init_context = match init_context {
            Ok(c) => Some(c),
            Err(e) => {
                return Some(Err(e));
            }
        };

        self.join_clauses
            .iter()
            .zip(self.join_columns.iter())
            .try_fold(
                init_context,
                |blend_context, (join_clause, (table, columns))| {
                    self.join(join_clause, table, columns, blend_context)
                },
            )
            .transpose()
    }

    fn join(
        &self,
        join_clause: &'a JoinClause,
        table: &'a Table,
        columns: &'a Vec<Column>,
        blend_context: Option<BlendContext<'a, T>>,
    ) -> Result<Option<BlendContext<'a, T>>> {
        let JoinClause {
            operator,
            constraint,
            ..
        } = join_clause;

        let where_clause = match constraint {
            JoinConstraint::On(where_clause) => Some(where_clause),
            JoinConstraint::Using(_) => {
                return Err(JoinError::UsingOnJoinNotSupported.into());
            }
        };
        let filter = Filter::new(self.storage, where_clause, self.filter_context);
        let blended_filter = BlendedFilter::new(&filter, blend_context.as_ref());

        let row = self
            .storage
            .get_data(&table.name)?
            .filter_map(move |item| {
                item.map_or_else(
                    |error| Some(Err(error)),
                    |(key, row)| {
                        blended_filter
                            .check(table, columns, &row)
                            .map(|pass| pass.as_some((columns, key, row)))
                            .transpose()
                    },
                )
            })
            .next()
            .transpose()?;

        let row = match row {
            Some((columns, key, row)) => Some(BlendContext {
                table,
                columns,
                key,
                row,
                next: blend_context.map(|c| Box::new(c)),
            }),
            None => match operator {
                JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin => blend_context,
                JoinOperator::Join | JoinOperator::InnerJoin => None,
                _ => {
                    return Err(JoinError::JoinTypeNotSupported.into());
                }
            },
        };

        Ok(row)
    }
}
