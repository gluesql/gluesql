use boolinator::Boolinator;
use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::{ColumnDef, Ident};

use super::filter::Filter;
use crate::data::Row;
use crate::result::Result;
use crate::store::Store;

#[derive(Error, Serialize, Debug, PartialEq)]
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

pub async fn fetch<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table_name: &'a str,
    columns: &'a [Ident],
    filter: Filter<'a, T>,
) -> Result<impl Iterator<Item = Result<(&'a [Ident], T, Row)>> + 'a> {
    let rows = storage.scan_data(table_name)?.filter_map(move |item| {
        item.map_or_else(
            |error| Some(Err(error)),
            |(key, row)| {
                filter
                    .check(&table_name, columns, &row)
                    .map(|pass| pass.as_some((columns, key, row)))
                    .transpose()
            },
        )
    });

    Ok(rows)
}
