// use boolinator::Boolinator;
use std::fmt::Debug;

use sqlparser::ast::{ColumnDef, Ident};

// use crate::data::Row;
// use crate::executor::Filter;
use crate::result::Result;
use crate::storage::Store;

pub fn fetch_columns<T: 'static + Debug>(
    storage: &dyn Store<T>,
    table_name: &str,
) -> Result<Vec<Ident>> {
    Ok(storage
        .get_schema2(table_name)?
        .column_defs
        .into_iter()
        .map(|ColumnDef { name, .. }| name)
        .collect::<Vec<Ident>>())
}

/*
pub fn fetch<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table: &'a Table,
    columns: &'a [Column],
    filter: Filter<'a, T>,
) -> Result<impl Iterator<Item = Result<(&'a [Column], T, Row)>> + 'a> {
    let rows = storage.get_data(&table.name)?.filter_map(move |item| {
        item.map_or_else(
            |error| Some(Err(error)),
            |(key, row)| {
                filter
                    .check(table, columns, &row)
                    .map(|pass| pass.as_some((columns, key, row)))
                    .transpose()
            },
        )
    });

    Ok(rows)
}
*/
