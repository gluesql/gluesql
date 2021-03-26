#![cfg(feature = "auto-increment")]
use {
    crate::{
        data::{schema::ColumnDefExt, Row},
        result::MutResult,
        store::{AlterTable, AutoIncrement, Store, StoreMut},
        Value,
    },
    futures::stream::{self, TryStreamExt},
    sqlparser::ast::ColumnDef,
    std::fmt::Debug,
};

pub async fn run<
    T: 'static + Debug,
    Storage: Store<T> + StoreMut<T> + AlterTable + AutoIncrement,
>(
    storage: Storage,
    rows: Vec<Row>,
    column_defs: &[ColumnDef],
    table_name: &str,
) -> MutResult<Storage, Vec<Row>> {
    let auto_increment_columns: Vec<(usize, &ColumnDef)> = column_defs
        .iter()
        .enumerate()
        .filter(|(_, column_def)| column_def.is_auto_incremented())
        .collect();

    stream::iter(auto_increment_columns.iter().map(Ok))
        .try_fold((storage, rows), |(storage, rows), column| async move {
            generate_column_values(storage, table_name, column, rows).await
        })
        .await
}

async fn generate_column_values<
    T: 'static + Debug,
    Storage: Store<T> + StoreMut<T> + AlterTable + AutoIncrement,
>(
    storage: Storage,
    table_name: &str,
    column: &(usize, &ColumnDef),
    rows: Vec<Row>,
) -> MutResult<Storage, Vec<Row>> {
    let (column_index, column_name) = *column;
    let column_name = column_name.name.value.as_str();

    let (storage, range) = storage
        .generate_values(table_name, column_name, rows.len())
        .await?;

    // FAIL: No-mut
    let mut rows = rows;
    let start = range.start;
    for value in range {
        rows[(value - start) as usize].0[column_index] = Value::I64(value);
    }

    Ok((storage, rows))
}
