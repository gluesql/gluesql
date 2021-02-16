use im_rc::HashSet;
use serde::Serialize;
use sqlparser::ast::{ColumnDef, ColumnOption, DataType, Ident};
use std::{convert::TryInto, fmt::Debug, rc::Rc};
use thiserror::Error as ThisError;

use crate::data::{Row, Schema, Value};
use crate::result::Result;
use crate::store::Store;
use crate::utils::Vector;

pub enum ColumnValidation {
    All,
    SpecifiedColumns(Vec<Ident>),
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

    #[error("conflict! column '{0}' of data type '{1}' conflicts with unique constraint")]
    ConflictOnUniqueColumnDataType(String, String),

    #[error("duplicate entry '{0}' for unique column '{1}'")]
    DuplicateEntryOnUniqueField(String, String),

    #[error("table already exists")]
    TableAlreadyExists,

    #[error("table does not exist")]
    TableNotExists,

    #[error("column '{0}' of data type '{1}' is unsupported for unique constraint")]
    UnsupportedDataTypeForUniqueColumn(String, String),
}

pub async fn validate_rows<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    validate_unique(storage, table_name, column_validation, row_iter).await
}

pub async fn validate_table<T: 'static + Debug>(
    storage: &impl Store<T>,
    schema: &Schema,
    if_not_exists: bool,
) -> Result<()> {
    validate_column_unique_option(&schema.column_defs)?;
    validate_table_if_not_exists(storage, &schema.table_name, if_not_exists).await
}

#[derive(Debug)]
struct UniqueConstraint<'a> {
    column_index: usize,
    column_def: &'a ColumnDef,
    keys: HashSet<UniqueKey>,
}

impl<'a> UniqueConstraint<'a> {
    fn new(column_index: usize, column_def: &'a ColumnDef) -> Self {
        Self {
            column_index,
            column_def,
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
            column_def: self.column_def,
            keys,
        })
    }

    fn check(&self, value: &Value) -> Result<UniqueKey> {
        let new_key = match value.try_into() {
            Ok(key) => key,
            Err(_) => {
                let column_def = self.column_def;
                return Err(ValidateError::ConflictOnUniqueColumnDataType(
                    column_def.name.to_string(),
                    column_def.data_type.to_string(),
                )
                .into());
            }
        };

        if new_key != UniqueKey::Null && self.keys.contains(&new_key) {
            // The input values are duplicate.
            return Err(ValidateError::DuplicateEntryOnUniqueField(
                format!("{:?}", value),
                self.column_def.name.to_string(),
            )
            .into());
        }

        Ok(new_key)
    }
}

fn create_unique_constraints<'a, 'b>(
    unique_columns: &[(usize, &'a ColumnDef)],
    row_iter: impl Iterator<Item = &'b Row> + Clone,
) -> Result<Vector<UniqueConstraint<'a>>> {
    unique_columns
        .iter()
        .try_fold(Vector::new(), |constraints, &col| {
            let (col_idx, col_def) = col;
            let new_constraint = UniqueConstraint::new(col_idx, col_def);
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

fn fetch_all_unique_columns(column_defs: &[ColumnDef]) -> Vec<(usize, &ColumnDef)> {
    column_defs
        .iter()
        .enumerate()
        .filter(|(_i, col)| {
            col.options
                .iter()
                .any(|opt_def| matches!(opt_def.option, ColumnOption::Unique { .. }))
        })
        .collect()
}

fn fetch_specified_unique_columns<'a>(
    column_defs: &'a [ColumnDef],
    specified_columns: &[Ident],
) -> Vec<(usize, &'a ColumnDef)> {
    column_defs
        .iter()
        .enumerate()
        .filter(|(_i, table_col)| {
            table_col
                .options
                .iter()
                .any(|opt_def| match opt_def.option {
                    ColumnOption::Unique { .. } => specified_columns
                        .iter()
                        .any(|specified_col| specified_col.value == table_col.name.value),
                    _ => false,
                })
        })
        .collect()
}

fn validate_column_unique_option(column_defs: &[ColumnDef]) -> Result<()> {
    let found = column_defs.iter().find(|col| match col.data_type {
        DataType::Float(_) => col
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::Unique { .. })),
        _ => false,
    });

    if let Some(col) = found {
        return Err(ValidateError::UnsupportedDataTypeForUniqueColumn(
            col.name.to_string(),
            col.data_type.to_string(),
        )
        .into());
    }

    Ok(())
}

async fn validate_table_if_not_exists<'a, T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    if_not_exists: bool,
) -> Result<()> {
    if if_not_exists || storage.fetch_schema(table_name).await?.is_none() {
        return Ok(());
    }

    Err(ValidateError::TableAlreadyExists.into())
}

async fn validate_unique<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    let Schema { column_defs, .. } = storage
        .fetch_schema(table_name)
        .await?
        .ok_or(ValidateError::TableNotExists)?;

    let unique_columns = match column_validation {
        ColumnValidation::All => fetch_all_unique_columns(&column_defs),
        ColumnValidation::SpecifiedColumns(columns) => {
            fetch_specified_unique_columns(&column_defs, &columns)
        }
    };

    let unique_constraints: Vec<_> = create_unique_constraints(&unique_columns, row_iter)?.into();
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
