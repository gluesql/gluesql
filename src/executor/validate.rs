use im_rc::HashSet;
use serde::Serialize;
use sqlparser::ast::{ColumnDef, DataType, ColumnOption, Ident};
use std::{convert::TryInto, fmt::Debug, rc::Rc};
use thiserror::Error as ThisError;

use crate::data::{Row, Value};
use crate::result::Result;
use crate::store::Store;
use crate::utils::Vector;

pub enum ColumnValidation {
    All(Rc<[ColumnDef]>),
    SpecifiedColumns(Rc<[ColumnDef]>, Vec<Ident>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum UniqueKey {
    Bool(bool),
    I64(i64),
    Str(String),
    Null,
}

#[derive(Debug, PartialEq, Serialize, ThisError)]
pub enum ValidateError {
    #[error("conflict! storage row has no column on index {0}")]
    ConflictOnStorageColumnIndex(usize),

    #[error("duplicate entry '{0}' for unique column '{1}'")]
    DuplicateEntryOnUniqueField(String, String),
}

pub async fn validate_rows<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    validate_unique(storage, table_name, column_validation, row_iter).await?;
    validate_type(storage, table_name, column_validation, row_iter).await?;
    Ok(())
}

#[derive(Debug)]
pub enum Constraint {
    UniqueConstraint {
        column_index: usize,
        column_name: String,
        keys: HashSet<UniqueKey>,
    },
    TypeConstraint {
        column_index: usize,
        column_name: String,
        keys: HashSet<UniqueKey>,
    },
}

impl Constraint {
    fn new(column_index: usize, column_name: String) -> Self {
        Self {
            column_index,
            column_name,
            keys: HashSet::new(),
        }
    }

    fn add(self, value: &Value) -> Result<Self> {
        let new_key = self.check(value)?;
        if new_key == UniqueKey::Null {
            return Ok(self);
        }

        let keys = self.keys.update(new_key);

        Ok(Self {
            column_index: self.column_index,
            column_name: self.column_name,
            keys,
        })
    }

    fn check(&self, value: &Value) -> Result<UniqueKey> {
        let new_key = value.try_into()?;
        if new_key != UniqueKey::Null && self.keys.contains(&new_key) {
            // The input values are duplicate.
            return Err(ValidateError::DuplicateEntryOnUniqueField(
                format!("{:?}", value),
                self.column_name.to_owned(),
            )
            .into());
        }

        Ok(new_key)
    }
}

fn create_constraints<'a>(
    columns: Vec<(usize, String)>,
    row_iter: impl Iterator<Item = &'a Row> + Clone,
    constraint: Constraint,
) -> Result<Vector<UniqueConstraint>> {
    unique_columns
        .into_iter()
        .try_fold(Vector::new(), |constraints, col| {
            let (col_idx, col_name) = col;
            let new_constraint = UniqueConstraint::new(col_idx, col_name);
            let new_constraint = row_iter
                .clone()
                .try_fold(new_constraint, |constraint, row| {
                    let val = row
                        .get_value(col_idx)
                        .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;
                    constraint.add(val)
                })?;
            Ok(constraints.push(new_constraint))
        })
}

fn fetch_all_unique_columns(column_defs: &[ColumnDef]) -> Vec<(usize, String)> {
    column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if table_col
                .options
                .iter()
                .any(|opt_def| matches!(opt_def.option, ColumnOption::Unique { .. }))
            {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}

fn fetch_all_columns_of_type(column_defs: &[ColumnDef], type: DataType) -> Vec<(usize, String)> {
    column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if matches!(table_col.data_type, type) {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}

fn fetch_specified_columns_of_type(
    all_column_defs: &[ColumnDef],
    specified_columns: &[Ident],
) -> Vec<(usize, String)> {
    column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if matches!(table_col.data_type, type) && specified_columns.any(|specified_col| specified_col.value == table_col.name.value) {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}

// KG: Made this so that code isn't repeated... Perhaps this is inefficient though?
fn specified_columns_only(
    matched_columns: Vec<(usize, String)>,
    specified_columns: &[Ident],
) -> Vec<(usize, String)> {
    matched_columns
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if specified_columns.any(|specified_col| specified_col.value == table_col.name.value) {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}

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