mod error;
mod schemaful;
mod schemaless;
mod values;

use crate::{
    data::{Key, Schema, Value},
    plan::{QueryPlan, TableColumnsPlan},
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
    table_columns: &TableColumnsPlan,
) -> Result<usize> {
    let column_defs = match table_columns {
        TableColumnsPlan::Unplanned => return Err(InsertError::UnplannedTableColumns.into()),
        TableColumnsPlan::Schemaless => None,
        TableColumnsPlan::Columns(column_defs) => Some(column_defs),
    };
    let Schema { foreign_keys, .. } = storage
        .fetch_schema(table_name)?
        .ok_or_else(|| InsertError::TableNotFound(table_name.to_owned()))?;

    let rows = match column_defs {
        None => schemaless::fetch_rows(storage, source).map(RowsData::Append),
        Some(column_defs) => schemaful::fetch_rows(
            storage,
            table_name,
            column_defs,
            columns,
            source,
            foreign_keys,
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

#[cfg(test)]
mod tests {
    use {
        super::{InsertError, insert},
        crate::{
            mock::MockStorage,
            plan::{QueryPlan, TableColumnsPlan, ValuesPlan},
            result::Error,
        },
    };

    #[test]
    fn unplanned_table_columns_are_rejected() {
        let mut storage = MockStorage::default();
        let source = QueryPlan::Values(ValuesPlan(Vec::new()));
        let actual = insert(
            &mut storage,
            "Item",
            &[],
            &source,
            &TableColumnsPlan::Unplanned,
        );

        assert_eq!(
            actual,
            Err(Error::Insert(InsertError::UnplannedTableColumns))
        );
    }
}
