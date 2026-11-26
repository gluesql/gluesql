use {
    crate::{
        data::{Key, Value},
        result::Result,
        store::{ScanColumns, Store},
    },
    im::HashSet,
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error as ThisError,
};

#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum UniqueError {
    #[error("conflict! storage row has no column on index {0}")]
    ConflictOnStorageColumnIndex(usize),

    #[error("duplicate entry '{}' for unique column '{1}'", String::from(.0))]
    DuplicateEntryOnUniqueField(Value, String),
}

#[derive(Debug)]
struct UniqueConstraint {
    column_index: usize,
    column_name: String,
    keys: HashSet<Key>,
}

impl UniqueConstraint {
    fn new(column_index: usize, column_name: String) -> Self {
        Self {
            column_index,
            column_name,
            keys: HashSet::new(),
        }
    }

    fn add(self, value: &Value) -> Result<Self> {
        let new_key = self.check(value)?;

        if matches!(new_key, Key::None) {
            return Ok(self);
        }

        let keys = self.keys.update(new_key);

        Ok(Self {
            column_index: self.column_index,
            column_name: self.column_name,
            keys,
        })
    }

    fn check(&self, value: &Value) -> Result<Key> {
        let key = Key::try_from(value)?;

        if self.keys.contains(&key) {
            Err(
                UniqueError::DuplicateEntryOnUniqueField(value.clone(), self.column_name.clone())
                    .into(),
            )
        } else {
            Ok(key)
        }
    }
}

pub(super) fn validate_unique_columns<'a, T: Store>(
    storage: &T,
    table_name: &str,
    columns: Vec<(usize, String)>,
    row_iter: &(impl Iterator<Item = &'a [Value]> + Clone),
) -> Result<()> {
    let unique_constraints = create_unique_constraints(columns, row_iter)?;
    if unique_constraints.is_empty() {
        return Ok(());
    }

    for row in storage.scan_data(table_name, ScanColumns::All)? {
        let (_, values) = row?;
        for constraint in &unique_constraints {
            let col_idx = constraint.column_index;
            let val = values
                .get(col_idx)
                .ok_or(UniqueError::ConflictOnStorageColumnIndex(col_idx))?;

            constraint.check(val)?;
        }
    }

    Ok(())
}

fn create_unique_constraints<'a>(
    unique_columns: Vec<(usize, String)>,
    row_iter: &(impl Iterator<Item = &'a [Value]> + Clone),
) -> Result<Vec<UniqueConstraint>> {
    let mut constraints = Vec::with_capacity(unique_columns.len());

    for (col_idx, col_name) in unique_columns {
        let new_constraint = UniqueConstraint::new(col_idx, col_name);
        let new_constraint = row_iter
            .clone()
            .try_fold(new_constraint, |constraint, row| {
                let val = row
                    .get(col_idx)
                    .ok_or(UniqueError::ConflictOnStorageColumnIndex(col_idx))?;

                constraint.add(val)
            })?;

        constraints.push(new_constraint);
    }

    Ok(constraints)
}
