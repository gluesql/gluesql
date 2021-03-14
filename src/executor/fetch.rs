use boolinator::Boolinator;
use futures::stream::{self, TryStream, TryStreamExt};
use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error as ThisError;

use sqlparser::ast::{ColumnDef, Ident};

use super::context::BlendContext;
use super::filter::index;
use super::filter::Filter;
use crate::data::Row;
use crate::result::{Error, Result};
use crate::store::{Index, Store};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum FetchError {
    #[error("table not found: {0}")]
    TableNotFound(String),
}

pub async fn fetch_columns<T: 'static + Debug + Eq + Ord, U: Store<T> + Index<T>>(
    storage: &U,
    table_name: &str,
) -> Result<Vec<Ident>> {
    Ok(storage
        .fetch_schema(table_name)
        .await?
        .ok_or_else(|| FetchError::TableNotFound(table_name.to_string()))?
        .column_defs
        .into_iter()
        .map(|ColumnDef { name, .. }| name)
        .collect::<Vec<Ident>>())
}

pub async fn fetch<'a, T: 'static + Debug + Eq + Ord, U: Store<T> + Index<T>>(
    storage: &'a U,
    table_name: &'a str,
    columns: Rc<[Ident]>,
    filter: Filter<'a, T>,
) -> Result<impl TryStream<Ok = (Rc<[Ident]>, T, Row), Error = Error> + 'a> {
    #[cfg(feature = "index")]
    let rows = index::fetch(storage, table_name, columns, filter);

    #[cfg(not(feature = "index"))]
    let filter = Rc::new(filter);
    #[cfg(not(feature = "index"))]
    let rows = storage
        .scan_data(table_name)
        .await
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
