use im_rc::HashSet;
use serde::Serialize;
use sqlparser::ast::{ColumnDef, DataType, ColumnOption, Ident};
use std::{convert::TryInto, fmt::Debug, rc::Rc};
use thiserror::Error as ThisError;

use crate::data::{Row, Value};
use crate::result::Result;
use crate::store::Store;
use crate::utils::Vector;

async fn validate_unique<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    let unique_columns = match column_validation {
        ColumnValidation::All(column_defs) => fetch_all_unique_columns(&column_defs),
        ColumnValidation::SpecifiedColumns(all_column_defs, specified_columns) => {
            specified_columns_only(fetch_all_unique_columns(&column_defs), &specified_columns)
        }
    };

    let unique_constraints: Vec<_> = create_unique_constraints(unique_columns, row_iter)?.into();
    if unique_constraints.is_empty() {
        return Ok(());
    }

    let unique_constraints = Rc::new(unique_constraints);
    storage.scan_data(table_name).await?.try_for_each(|result| {
        let (_, row) = result?;
        Rc::clone(&unique_constraints)
            .iter()
            .try_for_each(|constraint| {
                let col_idx = constraint.column_index;
                let val = row
                    .get_value(col_idx)
                    .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;
                constraint.check(val)?;
                Ok(())
            })
    })
}

async fn validate_type<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    validate_specific_type(storage, table_name, column_validation, row_iter, DataType::Boolean)?;
    validate_specific_type(storage, table_name, column_validation, row_iter, DataType::Int)?;
    validate_specific_type(storage, table_name, column_validation, row_iter, DataType::Float)?;
    validate_specific_type(storage, table_name, column_validation, row_iter, DataType::Text)?;
    Ok(())
}

async fn validate_specific_type<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
    type: DataType
) -> Result<()> {
    let columns = match column_validation {
        ColumnValidation::All(column_defs) => fetch_all_columns_of_type(&column_defs, type),
        ColumnValidation::SpecifiedColumns(all_column_defs, specified_columns) => {
            specified_columns_only(fetch_all_columns_of_type(&all_column_defs, type), &specified_columns)
        }
    };

    let constraints: Vec<_> = create_unique_constraints(columns, row_iter)?.into();
    if constraints.is_empty() {
        return Ok(());
    }

    let constraints = Rc::new(constraints);
    storage.scan_data(table_name).await?.try_for_each(|result| {
        let (_, row) = result?;
        Rc::clone(&constraints)
            .iter()
            .try_for_each(|constraint| {
                let col_idx = constraint.column_index;
                let val = row
                    .get_value(col_idx)
                    .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;
                constraint.check(val)?;
                Ok(())
            })
    })
}