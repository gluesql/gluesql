use {
    super::{UniqueKey, ValidateError},
    crate::{
        data::{Row, Value},
        result::Result,
        utils::Vector,
    },
    im_rc::HashSet,
    sqlparser::ast::{ColumnDef, ColumnOption, Ident},
    std::{convert::TryInto, fmt::Debug},
};

#[derive(Debug)]
pub struct UniqueConstraint {
    pub column_index: usize,
    pub column_name: String,
    pub keys: HashSet<UniqueKey>,
}

impl UniqueConstraint {
    pub fn new(column_index: usize, column_name: String) -> Self {
        Self {
            column_index,
            column_name,
            keys: HashSet::new(),
        }
    }

    pub fn add(self, value: &Value) -> Result<Self> {
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

    pub fn check(&self, value: &Value) -> Result<UniqueKey> {
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

pub fn create_unique_constraints<'a>(
    unique_columns: Vec<(usize, String)>,
    row_iter: impl Iterator<Item = &'a Row> + Clone,
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

pub fn fetch_all_unique_columns(column_defs: &[ColumnDef]) -> Vec<(usize, String)> {
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

// KG: Made this so that code isn't repeated... Perhaps this is inefficient though?
// KG: Unsure if we should keep this, I like how it works, code-wise and may be good if ever anything else needs specified columns.
pub fn specified_columns_only(
    matched_columns: Vec<(usize, String)>,
    specified_columns: &[Ident],
) -> Vec<(usize, String)> {
    matched_columns
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if specified_columns
                .iter()
                .any(|specified_col| specified_col.value == table_col.1)
            {
                Some((i, table_col.1.clone()))
            } else {
                None
            }
        })
        .collect()
}
