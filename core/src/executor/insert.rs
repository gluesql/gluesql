mod error;
mod schemaful;
mod schemaless;
mod values;

use crate::{
    data::{Key, Value},
    executor::context::StatementContext,
    plan::{QueryPlan, SchemaPlan},
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
    stmt_context: StatementContext<'_>,
    table_name: &str,
    columns: &[String],
    source: &QueryPlan,
) -> Result<usize> {
    let SchemaPlan {
        column_defs,
        foreign_keys,
        ..
    } = stmt_context
        .schema_map
        .get(table_name)
        .ok_or_else(|| InsertError::TableNotFound(table_name.to_owned()))?;
    let column_defs = column_defs.as_deref();

    let rows = match column_defs {
        None => schemaless::fetch_rows(storage, stmt_context, source).map(RowsData::Append),
        Some(column_defs) => schemaful::fetch_rows(
            storage,
            stmt_context,
            table_name,
            column_defs,
            columns,
            foreign_keys.clone(),
            source,
        ),
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
