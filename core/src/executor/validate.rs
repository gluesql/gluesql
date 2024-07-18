use {
    crate::{
        ast::{ColumnDef, UniqueConstraint},
        data::{Key, Value},
        result::Result,
        store::{DataRow, Store},
    },
    futures::stream::TryStreamExt,
    im_rc::HashSet,
    itertools::Itertools,
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

    #[error("duplicate entry '{0:?}' for unique column '{1:?}'")]
    DuplicateEntryOnUniqueField(Vec<Value>, Vec<String>),

    #[error("duplicate entry for primary_key field, parsed key: '{0:?}', message: '{0:?}'")]
    DuplicateEntryOnPrimaryKeyField(Option<Key>, Option<String>),
}

impl ValidateError {
    /// Returns a new `ValidateError::DuplicateEntryOnUniqueField` variant.
    pub fn duplicate_entry_on_multi_unique_field(
        value: Vec<Value>,
        column_names: Vec<String>,
    ) -> Self {
        Self::DuplicateEntryOnUniqueField(value, column_names)
    }

    /// Returns a new `ValidateError::DuplicateEntryOnUniqueField` variant.
    pub fn duplicate_entry_on_single_unique_field(value: Value, column_name: String) -> Self {
        Self::duplicate_entry_on_multi_unique_field(vec![value], vec![column_name])
    }
}

pub enum ColumnValidation<'column_def> {
    /// `INSERT`
    All(&'column_def [ColumnDef]),
    /// `UPDATE`
    SpecifiedColumns(&'column_def [ColumnDef], Vec<String>),
}

#[derive(Debug)]
/// A validator for unique constraints.
struct UniqueConstraintValidator {
    column_indices: Vec<usize>,
    column_names: Vec<String>,
    keys: HashSet<Key>,
}

impl UniqueConstraintValidator {
    fn new(column_indices: Vec<usize>, column_names: Vec<String>) -> Self {
        Self {
            column_indices,
            column_names,
            keys: HashSet::new(),
        }
    }

    /// Adds a new value to the unique constraint.
    ///
    /// # Arguments
    /// * `value` - The value to add to the unique constraint.
    ///
    /// # Raises
    /// * `ValidateError::DuplicateEntryOnUniqueField` - If the value already exists in the unique constraint.
    fn add(self, value: Vec<Value>) -> Result<Self> {
        // If there is any Null Value, given that the acceptance
        // of Null Values is not defined in the UniqueConstraint.
        if value.iter().any(|v| v.is_null()) {
            return Ok(self);
        }

        let new_key = self.check(&value)?;

        let keys = self.keys.update(new_key);

        Ok(Self {
            column_indices: self.column_indices,
            column_names: self.column_names,
            keys,
        })
    }

    fn check(&self, value: &[Value]) -> Result<Key> {
        let key = Key::try_from(value.to_vec())?;

        if !self.keys.contains(&key) {
            Ok(key)
        } else {
            Err(ValidateError::DuplicateEntryOnUniqueField(
                value.to_vec(),
                self.column_names.to_owned(),
            )
            .into())
        }
    }
}

/// Returns the indices of the primary key columns.
///
/// # Arguments
/// * `column_defs` - The column definitions of the table.
pub fn get_primary_key_column_indices(column_defs: &[ColumnDef]) -> Vec<usize> {
    column_defs
        .iter()
        .positions(|column_def| column_def.is_primary())
        .collect()
}

/// Returns the key associated with the given row.
///
/// # Arguments
/// * `row` - The row to extract the key from.
/// * `primary_key_indices` - The indices of the primary key columns.
pub fn get_primary_key_from_row(row: &[Value], primary_key_indices: &[usize]) -> Result<Key> {
    Ok(match primary_key_indices.len() {
        0 => unreachable!("Primary key indices should not be empty"),
        1 => {
            Key::try_from(row.get(primary_key_indices[0]).ok_or_else(|| {
                ValidateError::ConflictOnStorageColumnIndex(primary_key_indices[0])
            })?)?
        }
        _ => Key::Composite(
            primary_key_indices
                .iter()
                .map(|&index| {
                    Key::try_from(
                        row.get(index)
                            .ok_or(ValidateError::ConflictOnStorageColumnIndex(index))?,
                    )
                })
                .collect::<Result<Vec<Key>>>()?,
        ),
    })
}

type Constraints = (Option<Vec<usize>>, Vec<(Vec<usize>, Vec<String>)>);

pub async fn validate_unique<T: Store>(
    storage: &T,
    table_name: &str,
    column_validation: ColumnValidation<'_>,
    unique_constraints: &[UniqueConstraint],
    row_iter: impl Iterator<Item = &[Value]> + Clone,
) -> Result<()> {
    // First, we retrieve the primary key indices and the unique columns to validate.
    // Specifically, we only care about validating the primary key indices in the case of an UPDATE
    // if the primary key columns are specified in the set of the columns being updated.
    let (primary_key_indices, unique_columns): Constraints = match &column_validation {
        ColumnValidation::All(column_defs) => {
            let primary_keys: Vec<usize> = get_primary_key_column_indices(column_defs);
            (
                if primary_keys.is_empty() {
                    None
                } else {
                    Some(primary_keys)
                },
                column_defs
                    .iter()
                    .enumerate()
                    .filter(|(_, column_def)| column_def.is_unique_not_primary())
                    .map(|(index, column_def)| (vec![index], vec![column_def.name.clone()]))
                    .chain(
                        unique_constraints
                            .iter()
                            .map(|unique_constraint| {
                                (
                                    unique_constraint
                                        .columns()
                                        .iter()
                                        .map(|column_name| {
                                            column_defs
                                                .iter()
                                                .position(|column_def| {
                                                    column_def.name == *column_name
                                                })
                                                .unwrap()
                                        })
                                        .collect(),
                                    unique_constraint.columns().to_vec(),
                                )
                            })
                            .collect::<Vec<_>>(),
                    )
                    .collect(),
            )
        }
        ColumnValidation::SpecifiedColumns(column_defs, specified_columns) => {
            // We only need to validate the primary keys if one of the columns composing the primary key is specified
            // in the set of the specified columns, otherwise we can skip the validation for the primary keys.
            let primary_keys_were_specified = column_defs.iter().any(|column_def| {
                column_def.is_primary() && specified_columns.contains(&column_def.name)
            });

            (
                if primary_keys_were_specified {
                    Some(
                        column_defs
                            .iter()
                            .positions(|column_def: &ColumnDef| column_def.is_primary())
                            .collect(),
                    )
                } else {
                    None
                },
                column_defs
                    .iter()
                    .enumerate()
                    .filter(|(_, column_def)| {
                        column_def.is_unique_not_primary()
                            && specified_columns.contains(&column_def.name)
                    })
                    .map(|(index, column_def)| (vec![index], vec![column_def.name.clone()]))
                    .chain(
                        unique_constraints
                            .iter()
                            .filter(|unique_constraint| {
                                unique_constraint
                                    .columns()
                                    .iter()
                                    .any(|column_name| specified_columns.contains(column_name))
                            })
                            .map(|unique_constraint| {
                                (
                                    unique_constraint
                                        .columns()
                                        .iter()
                                        .map(|column_name| {
                                            column_defs
                                                .iter()
                                                .position(|column_def| {
                                                    column_def.name == *column_name
                                                })
                                                .unwrap()
                                        })
                                        .collect(),
                                    unique_constraint.columns().to_vec(),
                                )
                            })
                            .collect::<Vec<_>>(),
                    )
                    .collect(),
            )
        }
    };

    // We then proceed to validate the primary keys.
    if let Some(primary_key_indices) = primary_key_indices {
        for row in row_iter.clone() {
            let primary_key = get_primary_key_from_row(row, &primary_key_indices)?;

            if storage
                .fetch_data(table_name, &primary_key)
                .await?
                .is_some()
            {
                return Err(ValidateError::DuplicateEntryOnPrimaryKeyField(
                    Some(primary_key),
                    None,
                )
                .into());
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
                let values = constraint
                    .column_indices
                    .iter()
                    .map(|&column_index| {
                        Ok(values
                            .get(column_index)
                            .ok_or(ValidateError::ConflictOnStorageColumnIndex(column_index))?
                            .clone())
                    })
                    .collect::<Result<Vec<_>>>()?;

                if values.iter().any(|v| v.is_null()) {
                    return Ok(());
                }

                constraint.check(&values)?;

                Ok(())
            })
        })
        .await
}

fn create_unique_constraints<'a>(
    unique_columns: Vec<(Vec<usize>, Vec<String>)>,
    row_iter: impl Iterator<Item = &'a [Value]> + Clone,
) -> Result<Vector<UniqueConstraintValidator>> {
    unique_columns
        .into_iter()
        .try_fold(Vector::new(), |constraints, col| {
            let (column_indices, column_names) = col;
            let new_constraint = UniqueConstraintValidator::new(column_indices, column_names);
            let new_constraint = row_iter
                .clone()
                .try_fold(new_constraint, |constraint, row| {
                    let values = constraint
                        .column_indices
                        .as_slice()
                        .iter()
                        .map(|column_index| {
                            Ok(row
                                .get(*column_index)
                                .ok_or(ValidateError::ConflictOnStorageColumnIndex(*column_index))?
                                .to_owned())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    constraint.add(values)
                })?;
            Ok(constraints.push(new_constraint))
        })
}
