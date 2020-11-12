use boolinator::Boolinator;
use futures::stream::{self, TryStream, TryStreamExt};
use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error as ThisError;

use sqlparser::ast::{ColumnDef, Ident};

use super::context::BlendContext;
use super::filter::Filter;
use crate::data::Row;
use crate::result::{Error, Result};
use crate::store::Store;

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum FetchError {
    #[error("table not found: {0}")]
    TableNotFound(String),
}

pub fn fetch_columns<T: 'static + Debug>(
    storage: &dyn Store<T>,
    table_name: &str,
) -> Result<Vec<Ident>> {
    Ok(storage
        .fetch_schema(table_name)?
        .ok_or_else(|| FetchError::TableNotFound(table_name.to_string()))?
        .column_defs
        .into_iter()
        .map(|ColumnDef { name, .. }| name)
        .collect::<Vec<Ident>>())
}

pub fn fetch<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table_name: &'a str,
    columns: Rc<Vec<Ident>>,
    filter: Filter<'a, T>,
) -> Result<impl TryStream<Ok = (Rc<Vec<Ident>>, T, Row), Error = Error> + 'a> {
    let filter = Rc::new(filter);

    let rows = storage
        .scan_data(table_name)
        .map(stream::iter)?
        .try_filter_map(move |(key, row)| {
            let columns = Rc::clone(&columns);
            let filter = Rc::clone(&filter);

            let context = Rc::new(BlendContext::new(table_name, columns, Some(row), None));

            // TODO: remove two unwrap() uses.
            async move {
                filter.check(Rc::clone(&context)).await.map(|pass| {
                    let context = Rc::try_unwrap(context).unwrap();

                    pass.as_some((context.columns, key, context.row.unwrap()))
                })
            }
        });

    Ok(rows)
}
