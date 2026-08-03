mod error;
mod schemaful;
mod schemaless;
mod values;

use crate::{
    data::{Key, Schema, Value},
    plan::QueryPlan,
    result::Result,
    store::{GStore, GStoreMut},
};
pub use error::InsertError;

enum RowsData {
    Append(Vec<Vec<Value>>),
    Insert(Vec<(Key, Vec<Value>)>),
}

pub fn insert<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    columns: &[String],
    source: &QueryPlan,
) -> Result<usize> {
    let Schema {
        column_defs,
        foreign_keys,
        ..
    } = storage
        .fetch_schema(table_name)?
        .ok_or_else(|| InsertError::TableNotFound(table_name.to_owned()))?;

    let rows = match column_defs {
        Some(column_defs) => schemaful::fetch_rows(
            storage,
            table_name,
            column_defs,
            columns,
            source,
            foreign_keys,
        ),
        None => schemaless::fetch_rows(storage, source).map(RowsData::Append),
    }?;

    match rows {
        RowsData::Append(rows) => {
            let num_rows = rows.len();

            storage.append_data(table_name, rows).map(|()| num_rows)
        }
        RowsData::Insert(rows) => {
            let num_rows = rows.len();

            storage.insert_data(table_name, rows).map(|()| num_rows)
        }
    }
}
