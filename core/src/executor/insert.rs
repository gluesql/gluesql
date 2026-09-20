mod conflict;
mod error;
mod schemaful;
mod schemaless;
mod values;

use crate::{
    data::{Key, Schema, Value},
    plan::{OnConflictPlan, QueryPlan},
    result::Result,
    store::{GStore, GStoreMut},
};
pub use error::InsertError;

enum RowsData {
    Append(Vec<Vec<Value>>),
    Insert(Vec<(Key, Vec<Value>)>),
}

/// The rows an insert writes: the new ones, and the ones already in the table that an
/// `ON CONFLICT DO UPDATE` overwrites.
struct Writes {
    rows: RowsData,
    updated: Vec<(Key, Vec<Value>)>,
}

pub fn insert<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    columns: &[String],
    source: &QueryPlan,
    on_conflict: Option<&OnConflictPlan>,
) -> Result<usize> {
    let Schema {
        column_defs,
        foreign_keys,
        ..
    } = storage
        .fetch_schema(table_name)?
        .ok_or_else(|| InsertError::TableNotFound(table_name.to_owned()))?;

    let writes = if let Some(column_defs) = column_defs {
        schemaful::fetch_rows(
            storage,
            table_name,
            column_defs,
            columns,
            source,
            foreign_keys,
            on_conflict,
        )?
    } else {
        // A schemaless table has no constraint to conflict on.
        if let Some(on_conflict) = on_conflict {
            conflict::reject_schemaless(on_conflict)?;
        }

        Writes {
            rows: RowsData::Append(schemaless::fetch_rows(storage, source)?),
            updated: Vec::new(),
        }
    };

    let Writes { rows, updated } = writes;
    let num_updated = updated.len();
    if !updated.is_empty() {
        storage.insert_data(table_name, updated)?;
    }

    match rows {
        RowsData::Append(rows) => {
            let num_rows = rows.len();

            storage
                .append_data(table_name, rows)
                .map(|()| num_rows + num_updated)
        }
        RowsData::Insert(rows) => {
            let num_rows = rows.len();

            storage
                .insert_data(table_name, rows)
                .map(|()| num_rows + num_updated)
        }
    }
}
