use boolinator::Boolinator;
use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Ident, Query, SetExpr, TableWithJoins};

use super::aggregate::Aggregate;
use super::blend::Blend;
use super::context::{BlendContext, FilterContext};
use super::fetch::fetch_columns;
use super::filter::Filter;
use super::join::Join;
use super::limit::Limit;
use crate::data::{Row, Table};
use crate::result::Result;
use crate::store::Store;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum SelectError {
    #[error("unimplemented! select on two or more than tables are not supported")]
    TooManyTables,

    #[error("unreachable!")]
    Unreachable,
}

macro_rules! err {
    ($err: expr) => {{
        return Err($err.into());
    }};
}

fn fetch_blended<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table: Table<'a>,
    columns: Rc<Vec<Ident>>,
) -> Result<impl Iterator<Item = Result<BlendContext<'a>>> + 'a> {
    let rows = storage.scan_data(table.get_name())?.map(move |data| {
        let (_, row) = data?;
        let row = Some(row);
        let columns = Rc::clone(&columns);

        Ok(BlendContext {
            table_alias: table.get_alias(),
            columns,
            row,
            next: None,
        })
    });

    Ok(rows)
}

pub fn select<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    query: &'a Query,
    filter_context: Option<&'a FilterContext<'a>>,
) -> Result<impl Iterator<Item = Result<Row>> + 'a> {
    let (table_with_joins, where_clause, projection, group_by, having) = match &query.body {
        SetExpr::Select(statement) => {
            let tables = &statement.from;
            let table_with_joins = match tables.len() {
                1 => &tables[0],
                0 => err!(SelectError::Unreachable),
                _ => err!(SelectError::TooManyTables),
            };

            (
                table_with_joins,
                statement.selection.as_ref(),
                statement.projection.as_ref(),
                &statement.group_by,
                statement.having.as_ref(),
            )
        }
        _ => err!(SelectError::Unreachable),
    };

    let TableWithJoins { relation, joins } = &table_with_joins;
    let table = Table::new(relation)?;

    let columns = fetch_columns(storage, table.get_name())?;
    let columns = Rc::new(columns);
    let join_columns = joins
        .iter()
        .map(|join| {
            let table_name = Table::new(&join.relation)?.get_name();
            let columns = fetch_columns(storage, table_name)?;

            Ok(Rc::new(columns))
        })
        .collect::<Result<_>>()?;
    let join_columns = Rc::new(join_columns);

    let join = Join::new(storage, joins, filter_context);
    let aggregate = Aggregate::new(storage, projection, group_by, having, filter_context);
    let blend = Blend::new(storage, projection);
    let filter = Filter::new(storage, where_clause, filter_context, None);
    let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref())?;

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
        .filter_map(move |(i, item)| limit.check(i).as_some(item));

    let rows = {
        let rows = aggregate.apply(rows)?;
        let rows = Box::new(rows);

        rows.map(move |blend_context| blend.apply(blend_context))
    };

    Ok(rows)
}
