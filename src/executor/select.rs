use boolinator::Boolinator;
// use nom_sql::{Column, JoinClause, JoinRightSide, SelectStatement, Table};
use nom_sql::SelectStatement;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Query, SetExpr, TableFactor};

use crate::data::Row;
// use crate::executor::join::JoinColumns;
// use crate::executor::{fetch_columns, Blend, BlendContext, Filter, FilterContext, Join, Limit};
use crate::executor::{fetch_columns, Filter, FilterContext};
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

    #[error("unreachable!")]
    Unreachable,
}

/*
struct SelectParams<'a> {
    pub table: &'a Table,
    pub columns: Vec<Column>,
    pub join_columns: Rc<JoinColumns<'a>>,
}

fn fetch_select_params<'a, T: 'static + Debug>(
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

            let item = (table, Rc::new(fetch_columns(storage, table)?));

            Ok(Rc::new(item))
        })
        .collect::<Result<_>>()?;

    Ok(SelectParams {
        table,
        columns,
        join_columns: Rc::new(join_columns),
    })
}

fn fetch_blended<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table: &'a Table,
    columns: Rc<Vec<Column>>,
) -> Result<impl Iterator<Item = Result<BlendContext<'a, T>>> + 'a> {
    let rows = storage.get_data(&table.name)?.map(move |data| {
        let (key, row) = data?;
        let columns = Rc::clone(&columns);

        Ok(BlendContext {
            table,
            columns,
            key,
            row,
            next: None,
        })
    });

    Ok(rows)
}
*/

macro_rules! err {
    ($err: expr) => {{
        return Err($err.into());
    }};
}

pub fn select2<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    query: &'a Query,
    filter_context: Option<&'a FilterContext<'a>>,
) -> Result<impl Iterator<Item = Result<Row>> + 'a> {
    let (table, where_clause) = match &query.body {
        SetExpr::Select(statement) => {
            let tables = &statement.from;
            let table = match tables.len() {
                1 => &tables[0].relation,
                0 => err!(SelectError::Unreachable),
                _ => err!(SelectError::TooManyTables),
            };

            (table, statement.selection.as_ref())
        }
        _ => err!(SelectError::Unreachable),
    };

    let table_name = match table {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => err!(SelectError::Unreachable),
    };

    let columns = fetch_columns(storage, &table_name)?;
    let columns = Rc::new(columns);
    let filter = Filter::new(storage, where_clause, filter_context);

    let rows = storage
        .get_data(&table_name)?
        .filter_map(move |item| {
            item.map_or_else(
                |error| Some(Err(error)),
                |(key, row)| {
                    let columns = Rc::clone(&columns);

                    filter
                        .check(table, &columns, &row)
                        .map(|pass| pass.as_some((columns, key, row)))
                        .transpose()
                },
            )
        })
        .map(|item| item.map(|(_, _, row)| row));

    Ok(rows)
}

pub fn select<'a, T: 'static + Debug>(
    _storage: &'a dyn Store<T>,
    _statement: &'a SelectStatement,
    _filter_context: Option<&'a FilterContext<'a>>,
) -> Result<impl Iterator<Item = Result<Row>> + 'a> {
    let rows = vec![Ok(Row(vec![]))].into_iter();

    Ok(rows)

    /*
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
    } = fetch_select_params(storage, statement)?;
    let columns = Rc::new(columns);

    let blend = Blend::new(fields);
    let filter = Filter::new(storage, where_clause.as_ref(), filter_context);
    let join = Join::new(storage, join_clauses, filter_context);
    let limit = Limit::new(limit_clause);

    let rows = fetch_blended(storage, table, columns)?
        .flat_map(move |blend_context| {
            let join_columns = Rc::clone(&join_columns);

            join.apply(blend_context, join_columns)
        })
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

    Ok(rows)
    */
}
