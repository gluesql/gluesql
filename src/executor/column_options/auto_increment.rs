#![cfg(feature = "auto-increment")]
use {
    crate::{
        data::{schema::ColumnDefExt, Row},
        result::MutResult,
        store::{AlterTable, AutoIncrement, Store, StoreMut},
    },
    sqlparser::ast::ColumnDef,
    std::fmt::Debug,
};

pub async fn run<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable + AutoIncrement>(
    storage: U,
    rows: Vec<Row>,
    column_defs: &[ColumnDef],
    table_name: &str,
) -> MutResult<U, Vec<Row>> {
    let auto_increment_columns: Vec<(usize, &ColumnDef)> = column_defs
        .iter()
        .enumerate()
        .filter(|(_, column_def)| column_def.is_auto_incremented())
        .collect();

    if !auto_increment_columns.is_empty() {
        storage
            .generate_values(table_name, auto_increment_columns, rows)
            .await
    } else {
        Ok((storage, rows))
    }
}
