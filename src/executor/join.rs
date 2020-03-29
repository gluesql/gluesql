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
        init_context: Box<BlendContext<'a, T>>,
    ) -> Option<Result<BlendContext<'a, T>>> {
        let init_context = Some(Ok(*init_context));
        let join_zipped = self.join_clauses.iter().zip(self.join_columns.iter());

        join_zipped.fold(
            init_context,
            |blend_context, (join_clause, (table, columns))| match blend_context {
                Some(Ok(blend_context)) => self
                    .join(join_clause, table, columns, blend_context)
                    .transpose(),
                _ => blend_context,
            },
        )
    }

    fn join(
        &self,
        join_clause: &'a JoinClause,
        table: &'a Table,
        columns: &'a Vec<Column>,
        blend_context: BlendContext<'a, T>,
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
        let blended_filter = BlendedFilter::new(&filter, &blend_context);

        let row = self
            .storage
            .get_data(&table.name)?
            .map(move |(key, row)| (columns, key, row))
            .filter_map(move |item| {
                let (columns, _, row) = &item;

                blended_filter
                    .check(Some((table, columns, row)))
                    .map(|pass| pass.as_some(item))
                    .transpose()
            })
            .next()
            .transpose()?;

        Ok(match row {
            Some((columns, key, row)) => Some(BlendContext {
                table,
                columns,
                key,
                row,
                next: Some(Box::new(blend_context)),
            }),
            None => match operator {
                JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin => Some(blend_context),
                JoinOperator::Join | JoinOperator::InnerJoin => None,
                _ => {
                    return Err(JoinError::JoinTypeNotSupported.into());
                }
            },
        })
    }
}
