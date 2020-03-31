use boolinator::Boolinator;
use nom_sql::{Column, JoinClause, JoinRightSide, SelectStatement, Table};
use std::fmt::Debug;
use thiserror::Error;

use crate::data::Row;
use crate::executor::{fetch_columns, Blend, BlendContext, Filter, FilterContext, Join, Limit};
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
) -> Result<Box<dyn Iterator<Item = Result<BlendContext<'a, T>>> + 'a>> {
    let rows = storage.get_data(&table.name)?.map(move |data| {
        let (key, row) = data?;

        Ok(BlendContext {
            table,
            columns,
            key,
            row,
            next: None,
        })
    });

    Ok(Box::new(rows))
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
    let join = Join::new(storage, join_clauses, join_columns, filter_context);
    let limit = Limit::new(limit_clause);

    let rows = fetch_blended(storage, table, columns)?
        .filter_map(move |blend_context| join.apply(blend_context))
        .filter_map(move |blend_context| {
            blend_context.map_or_else(
                |error| Some(Err(error)),
                |blend_context| {
                    filter
                        .check_blended(&blend_context)
                        .map(|pass| pass.as_some(blend_context))
                        .transpose()
                },
            )
        })
        .enumerate()
        .filter_map(move |(i, item)| limit.check(i).as_some(item))
        .map(move |blend_context| blend.apply(blend_context));

    Ok(Box::new(rows))
}
