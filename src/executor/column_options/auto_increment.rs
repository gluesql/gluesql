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

    // FAIL: No-mut
    let rows_len = rows.len() as i64;
    let (storage, column_values) = stream::iter(auto_increment_columns.iter().map(Ok))
        .try_fold(
            (storage, vec![]),
            |(storage, mut column_values), column| async move {
                let (column_index, column_name) = *column;
                let column_name = column_name.name.value.as_str();

                let start = try_into!(
                    storage,
                    storage.get_increment_value(table_name, column_name).await
                );

                column_values.push((column_index, start));

                storage
                    .set_increment_value(table_name, column_name, start + rows_len)
                    .await
                    .map(|(storage, _)| (storage, column_values))
            },
        )
        .await?;
    let mut rows = rows;
    let mut column_values = column_values;
    for row in &mut rows {
        for column_value in &mut column_values {
            row.0[column_value.0] = Value::I64(column_value.1);
            column_value.1 += 1;
        }
    }
    Ok((storage, rows))
}
