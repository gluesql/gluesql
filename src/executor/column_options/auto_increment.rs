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

macro_rules! try_into {
    ($self: expr, $expr: expr) => {
        match $expr {
            Err(e) => {
                return Err(($self, e));
            }
            Ok(v) => v,
        }
    };
}

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
            generate_column_values(storage, table_name, column, rows).await // Ideally we would do one transaction per (current: AI columns * 2)
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

    let start = try_into!(
        storage,
        storage.get_increment_value(table_name, column_name).await
    );

    // FAIL: No-mut
    let mut rows = rows;
    let mut current = start;
    for row in &mut rows {
        row.0[column_index] = Value::I64(current.clone());
        current += 1;
    }

    storage
        .set_increment_value(table_name, column_name, current)
        .await
        .map(|(storage, _)| (storage, rows))
}
