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

    #[error("duplicate entry '{0:?}' for unique column '{1:?}'")]
    DuplicateEntryOnUniqueField(Vec<Value>, Vec<String>),

    #[error("duplicate entry for primary_key field, parsed key: '{0:?}', message: '{0:?}'")]
    DuplicateEntryOnPrimaryKeyField(Key),

    #[error("duplicated unique constraint found")]
    DuplicatedUniqueConstraintFound,
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

pub enum ColumnValidation {
    /// `INSERT`
    All,
    /// `UPDATE`
    SpecifiedColumns(Vec<String>),
}

#[derive(Debug)]
/// A validator for unique constraints.
struct UniqueConstraintValidator {
    column_indices: Vec<usize>,
    column_names: Vec<String>,
    keys: HashSet<Key>,
}

impl UniqueConstraintValidator {
    fn new(column_indices: &[usize], column_names: &[&str]) -> Self {
        Self {
            column_indices: column_indices.to_vec(),
            column_names: column_names.iter().map(|&s| s.to_owned()).collect(),
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
                self.column_names.clone(),
            )
            .into())
        }
    }
}

type Constraints<'a> = (bool, Vec<(&'a [usize], Vec<&'a str>)>);

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

    let (validate_primary_key, unique_columns): Constraints = match &column_validation {
        ColumnValidation::All => (
            schema.primary_key.is_some(),
            schema.unique_constraint_columns_and_indices().collect(),
        ),
        ColumnValidation::SpecifiedColumns(specified_columns) => (
            schema.has_primary_key_columns(specified_columns),
            schema
                .unique_constraint_columns_and_indices()
                .filter(|(_, column_names)| {
                    specified_columns.iter().any(|specified_column| {
                        column_names
                            .iter()
                            .any(|column_name| column_name == specified_column)
                    })
                })
                .collect(),
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
    unique_columns: Vec<(&[usize], Vec<&str>)>,
    row_iter: impl Iterator<Item = &'a [Value]> + Clone,
) -> Result<Vector<UniqueConstraintValidator>> {
    unique_columns
        .into_iter()
        .try_fold(Vector::new(), |constraints, col| {
            let (column_indices, column_names) = col;
            let new_constraint = UniqueConstraintValidator::new(column_indices, &column_names);
            let new_constraint = row_iter
                .clone()
                .try_fold(new_constraint, |constraint, row| {
                    let values = constraint
                        .column_indices
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
