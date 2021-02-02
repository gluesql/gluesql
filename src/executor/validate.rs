use futures::stream::{self, TryStreamExt};
use sqlparser::ast::{ColumnDef, ColumnOption, Ident};
use std::{cmp::Ordering, fmt::Debug, rc::Rc};

use crate::data::{Row, Schema, Value, ValueError};
use crate::result::Result;
use crate::store::Store;

use super::execute::ExecuteError;

pub enum ColumnValidation {
    All,
    SpecifiedColumns(Vec<Ident>),
}

pub async fn validate_rows<'a, T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    rows: &'a [Row],
) -> Result<()> {
    validate_rows_by(storage, table_name, column_validation, rows, |r| r).await
}

pub async fn validate_rows_by<'a, T: 'static + Debug, U: 'static + Debug, F>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    items: &'a [U],
    item_to_row: F,
) -> Result<()>
where
    F: Fn(&U) -> &Row,
{
    if items.is_empty() {
        return Ok(());
    }

    validate_unique_by(storage, table_name, column_validation, items, item_to_row).await
}

async fn validate_unique_by<'a, T: 'static + Debug, U: 'static + Debug, F>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    items: &'a [U],
    item_to_row: F,
) -> Result<()>
where
    F: Fn(&U) -> &Row,
{
    let Schema { column_defs, .. } = storage
        .fetch_schema(table_name)
        .await?
        .ok_or(ExecuteError::TableNotExists)?;
    let column_indexes = match column_validation {
        ColumnValidation::All => fetch_all_unique_column_indexes(&column_defs),
        ColumnValidation::SpecifiedColumns(columns) => {
            fetch_specified_unique_column_indexes(&column_defs, &columns)
        }
    };
    let unique_checks = create_unique_checks(&column_defs, &column_indexes, items, item_to_row)?;
    if unique_checks.is_empty() {
        return Ok(());
    }

    let column_defs = Rc::new(column_defs);
    let unique_checks = Rc::new(unique_checks);
    storage
        .scan_data(table_name)
        .await
        .map(stream::iter)?
        .try_for_each(|(_, row)| {
            let column_defs = Rc::clone(&column_defs);
            let unique_checks = Rc::clone(&unique_checks);

            async move {
                for check in unique_checks.as_ref() {
                    let column_index = check.column_index;
                    let row_val = row.get_value(column_index).unwrap();
                    if check
                        .values
                        .binary_search_by(|v| compare_values_for_sort(v, row_val).unwrap())
                        .is_ok()
                    {
                        return Err(ValueError::DuplicateEntryOnUniqueField(
                            format!("{:?}", row_val),
                            column_defs[column_index].name.to_string(),
                        )
                        .into());
                    }
                }
                Ok(())
            }
        })
        .await
}

fn fetch_all_unique_column_indexes(column_defs: &[ColumnDef]) -> Vec<usize> {
    let mut column_indexes = vec![];
    for (i, col) in column_defs.iter().enumerate() {
        if col
            .options
            .iter()
            .any(|opt_def| matches!(opt_def.option, ColumnOption::Unique { .. }))
        {
            column_indexes.push(i);
        }
    }

    column_indexes
}

fn fetch_specified_unique_column_indexes(
    table_column_defs: &[ColumnDef],
    specified_columns: &[Ident],
) -> Vec<usize> {
    let mut column_indexes = vec![];
    for (i, table_col) in table_column_defs.iter().enumerate() {
        if table_col
            .options
            .iter()
            .any(|opt_def| match opt_def.option {
                ColumnOption::Unique { .. } => specified_columns
                    .iter()
                    .any(|specified_col| specified_col.value == table_col.name.value),
                _ => false,
            })
        {
            column_indexes.push(i);
        }
    }

    column_indexes
}

#[derive(Debug)]
struct UniqueCheck<'a> {
    column_index: usize,
    values: Vec<&'a Value>,
}

fn create_unique_checks<'a, U: 'static + Debug, F>(
    table_column_defs: &[ColumnDef],
    unique_column_indexes: &[usize],
    items: &'a [U],
    item_to_row: F,
) -> Result<Vec<UniqueCheck<'a>>>
where
    F: Fn(&U) -> &Row,
{
    let item_len = items.len();
    let mut checks = Vec::with_capacity(unique_column_indexes.len());
    for &column_index in unique_column_indexes {
        let mut values: Vec<&'a Value> = Vec::with_capacity(item_len);
        for item in items {
            let new_val = item_to_row(item).get_value(column_index).unwrap();
            match values.binary_search_by(|v| compare_values_for_sort(v, new_val).unwrap()) {
                Ok(_) => {
                    // The input values are duplicate.
                    return Err(ValueError::DuplicateEntryOnUniqueField(
                        format!("{:?}", new_val),
                        table_column_defs[column_index].name.to_string(),
                    )
                    .into());
                }
                Err(idx) => values.insert(idx, new_val),
            }
        }

        checks.push(UniqueCheck {
            column_index,
            values,
        });
    }

    Ok(checks)
}

// This function tries to implement efficient value sort of the same column
// (as same data type). The return ordering has no meaning for the value
// comparison. It is only used to find the specified value efficiently
// (for function `binary_search`). And it assumes no value is Float NAN.
// This function should be updated when adding new Value.
fn compare_values_for_sort(lhs: &Value, rhs: &Value) -> Option<Ordering> {
    if let Some(ordering) = lhs.partial_cmp(rhs) {
        return Some(ordering);
    }

    match (lhs, rhs) {
        (Value::OptBool(Some(_)), Value::OptBool(None))
        | (Value::OptI64(Some(_)), Value::OptI64(None))
        | (Value::OptF64(Some(_)), Value::OptF64(None))
        | (Value::OptStr(Some(_)), Value::OptStr(None)) => Some(Ordering::Greater),
        (Value::OptBool(None), Value::OptBool(Some(_)))
        | (Value::OptI64(None), Value::OptI64(Some(_)))
        | (Value::OptF64(None), Value::OptF64(Some(_)))
        | (Value::OptStr(None), Value::OptStr(Some(_))) => Some(Ordering::Less),
        (Value::Empty, Value::Empty) => Some(Ordering::Equal),
        (_, Value::Empty) => Some(Ordering::Greater),
        (Value::Empty, _) => Some(Ordering::Less),
        _ => None,
    }
}
