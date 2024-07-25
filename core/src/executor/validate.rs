use {
    crate::{
        data::{Key, Schema, Value},
        result::Result,
        store::{DataRow, Store},
    },
    futures::stream::TryStreamExt,
    im_rc::HashSet,
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error as ThisError,
    utils::Vector,
};

#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum ValidateError {
    #[error("conflict! storage row has no column on index {0}")]
    ConflictOnStorageColumnIndex(usize),

    #[error("conflict! schemaless row found in schema based data")]
    ConflictOnUnexpectedSchemalessRowFound,

    #[error("duplicate entry '{}' for unique column '{1}'", String::from(.0))]
    DuplicateEntryOnUniqueField(Value, String),

    #[error("duplicate entry for primary_key field, parsed key: '{0:?}', message: '{0:?}'")]
    DuplicateEntryOnPrimaryKeyField(Key),
}

pub enum ColumnValidation {
    /// `INSERT`
    All,
    /// `UPDATE`
    SpecifiedColumns(Vec<String>),
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

        if !self.keys.contains(&key) {
            Ok(key)
        } else {
            Err(ValidateError::DuplicateEntryOnUniqueField(
                value.clone(),
                self.column_name.to_owned(),
            )
            .into())
        }
    }
}

pub async fn validate_unique<T: Store>(
    storage: &T,
    table_name: &str,
    schema: &Schema,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &[Value]> + Clone,
) -> Result<()> {
    // First, we retrieve the primary key indices and the unique columns to validate.
    // Specifically, we only care about validating the primary key indices in the case of an UPDATE
    // if the primary key columns are specified in the set of the columns being updated.
    let (validate_primary_key, unique_columns): (bool, Vec<(usize, &str)>) =
        match &column_validation {
            ColumnValidation::All => (
                schema.primary_key.is_some(),
                schema
                    .column_defs
                    .as_ref()
                    .map_or_else(Vec::new, |column_defs| {
                        column_defs
                            .iter()
                            .enumerate()
                            .filter(|(_, column_def)| column_def.unique)
                            .map(|(index, column_def)| (index, column_def.name.as_str()))
                            .collect()
                    }),
            ),
            ColumnValidation::SpecifiedColumns(specified_columns) => (
                schema.has_primary_key_columns(specified_columns),
                schema
                    .column_defs
                    .as_ref()
                    .map_or_else(Vec::new, |column_defs| {
                        column_defs
                            .iter()
                            .enumerate()
                            .filter(|(_, column_def)| {
                                column_def.unique && specified_columns.contains(&column_def.name)
                            })
                            .map(|(index, column_def)| (index, column_def.name.as_str()))
                            .collect()
                    }),
            ),
        };

    // We then proceed to validate the primary keys.
    if validate_primary_key {
        for row in row_iter.clone() {
            let primary_key = schema.get_primary_key(row)?;

            if storage
                .fetch_data(table_name, &primary_key)
                .await?
                .is_some()
            {
                return Err(ValidateError::DuplicateEntryOnPrimaryKeyField(primary_key).into());
            }
        }
    }

    // After having validated the primary keys, we proceed to validate the unique columns.
    // If the unique columns are empty, we can skip the validation.
    if unique_columns.is_empty() {
        return Ok(());
    }

    let unique_constraints: Vec<_> = create_unique_constraints(unique_columns, row_iter)?.into();

    let unique_constraints = &unique_constraints;
    storage
        .scan_data(table_name)
        .await?
        .try_for_each(|(_, data_row)| async {
            let values = match data_row {
                DataRow::Vec(values) => values,
                DataRow::Map(_) => {
                    return Err(ValidateError::ConflictOnUnexpectedSchemalessRowFound.into());
                }
            };

            unique_constraints.iter().try_for_each(|constraint| {
                let col_idx = constraint.column_index;
                let val = values
                    .get(col_idx)
                    .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;

                constraint.check(val)?;

                Ok(())
            })
        })
        .await
}

fn create_unique_constraints<'a>(
    unique_columns: Vec<(usize, &str)>,
    row_iter: impl Iterator<Item = &'a [Value]> + Clone,
) -> Result<Vector<UniqueConstraint>> {
    unique_columns
        .into_iter()
        .try_fold(Vector::new(), |constraints, col| {
            let (col_idx, col_name) = col;
            let new_constraint = UniqueConstraint::new(col_idx, col_name.to_owned());
            let new_constraint = row_iter
                .clone()
                .try_fold(new_constraint, |constraint, row| {
                    let val = row
                        .get(col_idx)
                        .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;

                    constraint.add(val)
                })?;
            Ok(constraints.push(new_constraint))
        })
}
