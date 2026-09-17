mod error;
mod schemaful;
mod schemaless;
mod upsert;
mod values;

pub use error::InsertError;
use {
    super::{execute::Payload, returning},
    crate::{
        ast::ColumnDef,
        data::{Key, Schema, Value},
        executor::execute::ExecuteError,
        plan::{OnConflictPlan, QueryPlan, SelectItemPlan},
        result::Result,
        store::{GStore, GStoreMut},
    },
    std::rc::Rc,
};

enum RowsData {
    Append(Vec<Vec<Value>>),
    Insert(Vec<(Key, Vec<Value>)>),
}

/// The storage mutation an `INSERT` prepared but has not performed yet.
enum PendingWrite {
    Append(Vec<Vec<Value>>),
    Insert(Vec<(Key, Vec<Value>)>),
    Upsert(upsert::PendingWrite),
}

pub fn insert<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    columns: &[String],
    source: &QueryPlan,
    on_conflict: Option<&OnConflictPlan>,
    returning: Option<&[SelectItemPlan]>,
) -> Result<Payload> {
    let Schema {
        column_defs,
        foreign_keys,
        ..
    } = storage
        .fetch_schema(table_name)?
        .ok_or_else(|| InsertError::TableNotFound(table_name.to_owned()))?;

    let Some(column_defs) = column_defs else {
        if on_conflict.is_some() {
            return Err(InsertError::OnConflictOnSchemalessTable(table_name.to_owned()).into());
        }
        if returning.is_some() {
            return Err(ExecuteError::ReturningOnSchemalessTable(table_name.to_owned()).into());
        }

        let rows = schemaless::fetch_rows(storage, source)?;
        let num_rows = rows.len();

        return storage
            .append_data(table_name, rows)
            .map(|()| Payload::Insert(num_rows));
    };

    let column_defs: Rc<[ColumnDef]> = Rc::from(column_defs);
    let column_names: Rc<[String]> = column_defs
        .iter()
        .map(|column_def| column_def.name.clone())
        .collect::<Vec<_>>()
        .into();

    let labels = returning
        .map(|items| returning::labels(table_name, &column_names, items))
        .transpose()?;

    let (num_rows, affected_rows, write) = if let Some(on_conflict) = on_conflict {
        let rows = schemaful::build_rows(storage, &column_defs, columns, source)?;
        let upsert::UpsertOutcome {
            rows_affected,
            affected_rows,
            write,
        } = upsert::execute(
            storage,
            table_name,
            &column_defs,
            &foreign_keys,
            rows,
            on_conflict,
        )?;

        (
            rows_affected,
            Some(affected_rows),
            PendingWrite::Upsert(write),
        )
    } else {
        let rows = schemaful::fetch_rows(
            storage,
            table_name,
            column_defs.to_vec(),
            columns,
            source,
            foreign_keys,
        )?;

        match rows {
            RowsData::Append(rows) => {
                let num_rows = rows.len();
                let affected = returning.is_some().then(|| rows.clone());

                (num_rows, affected, PendingWrite::Append(rows))
            }
            RowsData::Insert(rows) => {
                let num_rows = rows.len();
                let affected = returning
                    .is_some()
                    .then(|| rows.iter().map(|(_, values)| values.clone()).collect());

                (num_rows, affected, PendingWrite::Insert(rows))
            }
        }
    };

    // The `RETURNING` payload is built before the storage mutation so that a
    // failing projection leaves the table untouched, which matters for the
    // storages that cannot roll a statement back.
    let payload = match (returning, labels) {
        (Some(items), Some(labels)) => Some(returning::build_payload(
            storage,
            table_name,
            &column_names,
            items,
            labels,
            affected_rows.unwrap_or_default(),
        )?),
        _ => None,
    };

    match write {
        PendingWrite::Append(rows) => storage.append_data(table_name, rows)?,
        PendingWrite::Insert(rows) => storage.insert_data(table_name, rows)?,
        PendingWrite::Upsert(write) => upsert::apply(storage, table_name, write)?,
    }

    Ok(payload.unwrap_or(Payload::Insert(num_rows)))
}
