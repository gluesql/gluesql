use boolinator::Boolinator;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Ident, ObjectName, Query, SetExpr, TableFactor, TableWithJoins};

use crate::data::Row;
use crate::executor::{fetch_columns, Blend, BlendContext, Filter, FilterContext, Join};
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

macro_rules! err {
    ($err: expr) => {{
        return Err($err.into());
    }};
}

fn fetch_blended<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table_name: &str,
    table_alias: &'a str,
    columns: Rc<Vec<Ident>>,
) -> Result<impl Iterator<Item = Result<BlendContext<'a, T>>> + 'a> {
    let rows = storage.get_data(table_name)?.map(move |data| {
        let (key, row) = data?;
        let columns = Rc::clone(&columns);

        Ok(BlendContext {
            table_alias,
            columns,
            key,
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
    let (table_with_joins, where_clause, projection) = match &query.body {
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
            )
        }
        _ => err!(SelectError::Unreachable),
    };

    fn get_table_name<'a>(relation: &'a TableFactor) -> Result<&'a String> {
        match relation {
            TableFactor::Table { name, .. } => {
                let ObjectName(idents) = name;

                idents
                    .last()
                    .map(|ident| &ident.value)
                    .ok_or_else(|| SelectError::Unreachable.into())
            }
            _ => err!(SelectError::Unreachable),
        }
    };

    let TableWithJoins { relation, joins } = &table_with_joins;
    let table_name = get_table_name(relation)?;
    let columns = fetch_columns(storage, &table_name)?;
    let columns = Rc::new(columns);
    let join_columns = joins
        .iter()
        .map(|join| {
            let table_name = get_table_name(&join.relation)?;
            let columns = fetch_columns(storage, table_name)?;

            Ok(Rc::new(columns))
        })
        .collect::<Result<_>>()?;
    let join_columns = Rc::new(join_columns);

    let join = Join::new(storage, joins, filter_context);
    let blend = Blend::new(projection);
    let filter = Filter::new(storage, where_clause, filter_context);

    let rows = fetch_blended(storage, &table_name, &table_name, columns)?
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
        // .enumerate()
        // .filter_map(move |(i, item)| limit.check(i).as_some(item))
        .map(move |blend_context| blend.apply(blend_context));

    Ok(rows)
}
