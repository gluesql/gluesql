use nom_sql::{
    Column, JoinClause, JoinConstraint, JoinOperator, JoinRightSide, SelectStatement, Table,
};
use std::fmt::Debug;
use thiserror::Error;

use crate::data::Row;
use crate::executor::{
    fetch_columns, Blend, BlendContext, BlendedFilter, Filter, FilterContext, Limit,
};
use crate::result::Result;
use crate::storage::Store;

#[derive(Error, Debug, PartialEq)]
pub enum SelectError {
    #[error("table not found")]
    TableNotFound,

    #[error("unimplemented! select on two or more than tables are not supported")]
    TooManyTables,

    #[error("unimplemented! join right side not supported")]
    JoinRightSideNotSupported,

    #[error("unimplemented! join not supported")]
    JoinTypeNotSupported,

    #[error("unimplemented! using on join not supported")]
    UsingOnJoinNotSupported,
}

pub struct SelectParams<'a> {
    pub table: &'a Table,
    pub columns: Vec<Column>,
    pub join_columns: Vec<(&'a Table, Vec<Column>)>,
}

pub fn fetch_select_params<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    statement: &'a SelectStatement,
) -> Result<SelectParams<'a>> {
    let SelectStatement {
        tables,
        join: join_clauses,
        ..
    } = statement;
    let table = tables
        .iter()
        .enumerate()
        .map(|(i, table)| match i {
            0 => Ok(table),
            _ => Err(SelectError::TooManyTables.into()),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .next()
        .ok_or(SelectError::TableNotFound)?;

    let columns = fetch_columns(storage, table)?;
    let join_columns = join_clauses
        .iter()
        .map(|JoinClause { right, .. }| {
            let table = match &right {
                JoinRightSide::Table(table) => Ok(table),
                _ => Err(SelectError::JoinRightSideNotSupported),
            }?;

            Ok((table, fetch_columns(storage, table)?))
        })
        .collect::<Result<_>>()?;

    Ok(SelectParams {
        table,
        columns,
        join_columns,
    })
}

fn fetch_blended<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table: &'a Table,
    columns: &'a Vec<Column>,
) -> Result<Box<dyn Iterator<Item = BlendContext<'a, T>> + 'a>> {
    let rows = storage
        .get_data(&table.name)?
        .map(move |(key, row)| (columns, key, row))
        .map(move |(columns, key, row)| BlendContext {
            table,
            columns,
            key,
            row,
            next: None,
        });

    Ok(Box::new(rows))
}

fn join<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    join_clause: &'a JoinClause,
    table: &'a Table,
    columns: &'a Vec<Column>,
    filter_context: Option<&'a FilterContext<'a>>,
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
            return Err(SelectError::UsingOnJoinNotSupported.into());
        }
    };
    let filter = Filter::new(storage, where_clause, filter_context);
    let blended_filter = BlendedFilter::new(&filter, &blend_context);

    let row = storage
        .get_data(&table.name)?
        .map(move |(key, row)| (columns, key, row))
        .filter(move |(columns, _, row)| blended_filter.check(Some((table, columns, row))))
        .next();

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
                return Err(SelectError::JoinTypeNotSupported.into());
            }
        },
    })
}

pub fn select<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    statement: &'a SelectStatement,
    params: &'a SelectParams<'a>,
    filter_context: Option<&'a FilterContext<'a>>,
) -> Result<Box<dyn Iterator<Item = Result<Row>> + 'a>> {
    let SelectStatement {
        where_clause,
        limit: limit_clause,
        join: join_clauses,
        fields,
        ..
    } = statement;
    let SelectParams {
        table,
        columns,
        join_columns,
    } = params;

    let blend = Blend::new(fields);
    let filter = Filter::new(storage, where_clause.as_ref(), filter_context);
    let limit = Limit::new(limit_clause);

    let rows = fetch_blended(storage, table, columns)?
        .filter_map(move |init_context| {
            let join_zipped = join_clauses.iter().zip(join_columns.iter());
            let init_context = Some(Ok(init_context));

            join_zipped.fold(
                init_context,
                |blend_context, (join_clause, (table, columns))| match blend_context {
                    Some(Ok(blend_context)) => join(
                        storage,
                        join_clause,
                        table,
                        columns,
                        filter_context,
                        blend_context,
                    )
                    .transpose(),
                    _ => blend_context,
                },
            )
        })
        .filter(move |blend_context| {
            // TODO: BlendedFilter should accept context as Result type
            let filter = &filter;

            blend_context.as_ref().map_or(true, move |blend_context| {
                BlendedFilter::new(&filter, &blend_context).check(None)
            })
        })
        .enumerate()
        .filter_map(move |(i, item)| match limit.check(i) {
            true => Some(item),
            false => None,
        })
        .map(move |blend_context| {
            // TODO: Blend should accept context as Result type
            let blend = &blend;

            blend_context.map(move |BlendContext { columns, row, .. }| blend.apply(&columns, row))
        });

    Ok(Box::new(rows))
}
