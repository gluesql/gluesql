use {
    crate::{
        data::{Key, Value},
        result::Result,
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
pub(super) struct UniqueConstraint {
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

    fn add(self, key: Key) -> Result<Self> {
        self.check_duplicate(&key)?;

        if matches!(key, Key::None) {
            return Ok(self);
        }

        let keys = self.keys.update(key);

        Ok(Self {
            column_index: self.column_index,
            column_name: self.column_name,
            keys,
        })
    }

    pub(super) fn key_from_row(&self, row: &[Value]) -> Result<Key> {
        let value = row
            .get(self.column_index)
            .ok_or(UniqueError::ConflictOnStorageColumnIndex(self.column_index))?;

        Key::try_from(value)
    }

    pub(super) fn check_duplicate(&self, key: &Key) -> Result<()> {
        if self.keys.contains(key) {
            Err(UniqueError::DuplicateEntryOnUniqueField(
                key.clone().into(),
                self.column_name.clone(),
            )
            .into())
        } else {
            Ok(())
        }
    }
}

pub(super) fn create_unique_constraints<'a>(
    unique_columns: Vec<(usize, String)>,
    row_iter: &(impl Iterator<Item = &'a [Value]> + Clone),
) -> Result<Vec<UniqueConstraint>> {
    let mut constraints = Vec::with_capacity(unique_columns.len());

    for (col_idx, col_name) in unique_columns {
        let new_constraint = UniqueConstraint::new(col_idx, col_name);
        let new_constraint = row_iter
            .clone()
            .try_fold(new_constraint, |constraint, row| {
                let key = constraint.key_from_row(row)?;
                constraint.add(key)
            })?;

        constraints.push(new_constraint);
    }

    Ok(constraints)
}
