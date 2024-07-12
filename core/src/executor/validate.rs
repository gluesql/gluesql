use {
    crate::{
        ast::ColumnDef,
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

    #[error("duplicate entry '{}' for unique column '{1}'", String::from(.0))]
    DuplicateEntryOnUniqueField(Value, String),

    #[error("duplicate entry '{0:?}' for primary_key field")]
    DuplicateEntryOnPrimaryKeyField(Key),
}

pub enum ColumnValidation<'column_def> {
    /// `INSERT`
    All(&'column_def [ColumnDef]),
    /// `UPDATE`
    SpecifiedColumns(&'column_def [ColumnDef], Vec<String>),
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
                            .ok_or_else(|| ValidateError::ConflictOnStorageColumnIndex(index))?,
                    )
                })
                .collect::<Result<Vec<Key>>>()?
                .into(),
        ),
    })
}

pub async fn validate_unique<T: Store>(
    storage: &T,
    table_name: &str,
    column_validation: ColumnValidation<'_>,
    row_iter: impl Iterator<Item = &[Value]> + Clone,
) -> Result<()> {
    // First, we retrieve the primary key indices and the unique columns to validate.
    // Specifically, we only care about validating the primary key indices in the case of an UPDATE
    // if the primary key columns are specified in the set of the columns being updated.
    let (primary_key_indices, unique_columns): (Option<Vec<usize>>, Vec<(usize, &str)>) =
        match &column_validation {
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
                        .map(|(index, column_def)| (index, column_def.name.as_str()))
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
                        .map(|(index, column_def)| (index, column_def.name.as_str()))
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
                dbg!("Found primary key: {:?}", &primary_key);
                return Err(ValidateError::DuplicateEntryOnPrimaryKeyField(primary_key).into());
            } else {
                dbg!("Primary key not found: {:?}, table_name: {:?}", &primary_key, &table_name);
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
