use im_rc::HashSet;
use serde::Serialize;
use sqlparser::ast::{ColumnDef, ColumnOption, DataType, Ident};
use std::{convert::TryInto, fmt::Debug, rc::Rc};
use thiserror::Error as ThisError;

use crate::data::{Row, Value};
use crate::result::Result;
use crate::store::Store;
use crate::utils::Vector;

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
    fn new(constraint: Constraint, column_index: usize, column_name: String) -> Self {
        match constraint {
            UniqueConstraint => UniqueConstraint {
                column_index,
                column_name,
                keys: HashSet::new(),
            },
            TypeConstraint => TypeConstraint {
                column_index,
                column_name,
                keys: HashSet::new(),
            },
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
) -> Result<Vector<Constraint>> {
    unique_columns
        .into_iter()
        .try_fold(Vector::new(), |constraints, col| {
            let (col_idx, col_name) = col;
            let new_constraint = Constraint::new(col_idx, col_name);
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
