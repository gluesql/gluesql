use {
    crate::{
        data::{Row, Value},
        result::Result,
        store::Store,
        utils::Vector,
    },
    boolinator::Boolinator,
    chrono::{NaiveDate, NaiveDateTime},
    im_rc::HashSet,
    serde::Serialize,
    sqlparser::ast::{ColumnDef, ColumnOption, Ident},
    std::{convert::TryInto, fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum ValidateError {
    #[error("conflict! storage row has no column on index {0}")]
    ConflictOnStorageColumnIndex(usize),

    #[error("duplicate entry '{0}' for unique column '{1}'")]
    DuplicateEntryOnUniqueField(String, String),
}

pub enum ColumnValidation {
    All(Rc<[ColumnDef]>),
    SpecifiedColumns(Rc<[ColumnDef]>, Vec<Ident>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum UniqueKey {
    Bool(bool),
    I64(i64),
    Str(String),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
}

#[derive(Debug)]
struct UniqueConstraint {
    column_index: usize,
    column_name: String,
    keys: HashSet<UniqueKey>,
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
        let new_key = match self.check(value)? {
            Some(new_key) => new_key,
            None => {
                return Ok(self);
            }
        };

        let keys = self.keys.update(new_key);

        Ok(Self {
            column_index: self.column_index,
            column_name: self.column_name,
            keys,
        })
    }

    fn check(&self, value: &Value) -> Result<Option<UniqueKey>> {
        match value.try_into()? {
            Some(new_key) => (!self.keys.contains(&new_key)).as_result_from(
                || Some(new_key),
                || {
                    ValidateError::DuplicateEntryOnUniqueField(
                        format!("{:?}", value),
                        self.column_name.to_owned(),
                    )
                    .into()
                },
            ),
            None => Ok(None),
        }
    }
}

pub async fn validate_unique<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    let columns = match column_validation {
        ColumnValidation::All(column_defs) => fetch_all_unique_columns(&column_defs),
        ColumnValidation::SpecifiedColumns(column_defs, specified_columns) => {
            fetch_specified_unique_columns(&column_defs, &specified_columns)
        }
    };

    let unique_constraints: Vec<_> = create_unique_constraints(columns, row_iter)?.into();
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

fn create_unique_constraints<'a>(
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

fn fetch_specified_unique_columns(
    all_column_defs: &[ColumnDef],
    specified_columns: &[Ident],
) -> Vec<(usize, String)> {
    all_column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if table_col
                .options
                .iter()
                .any(|opt_def| match opt_def.option {
                    ColumnOption::Unique { .. } => specified_columns
                        .iter()
                        .any(|specified_col| specified_col.value == table_col.name.value),
                    _ => false,
                })
            {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}
