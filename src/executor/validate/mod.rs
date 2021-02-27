mod unique;

use {
    crate::{data::Row, result::Result, store::Store},
    serde::Serialize,
    sqlparser::ast::{ColumnDef, Ident},
    std::{fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
    unique::{create_unique_constraints, fetch_all_unique_columns, specified_columns_only},
};

#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum ValidateError {
    #[error("conflict! storage row has no column on index {0}")]
    ConflictOnStorageColumnIndex(usize),

    #[error("duplicate entry '{0}' for unique column '{1}'")]
    DuplicateEntryOnUniqueField(String, String),

    #[error("incompatible type attempted, value: {attempted_value} used for typed column: {column_name} ({column_type})")]
    IncompatibleTypeOnTypedField {
        attempted_value: String,
        column_name: String,
        column_type: String,
    },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum UniqueKey {
    Bool(bool),
    I64(i64),
    Str(String),
    Null,
}

#[derive(Clone)]
pub enum ColumnValidation {
    All(Rc<[ColumnDef]>),
    SpecifiedColumns(Rc<[ColumnDef]>, Vec<Ident>),
}

pub async fn validate_rows<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    validate_unique(
        storage,
        table_name,
        column_validation.clone(),
        row_iter.clone(),
    )
    .await?;
    validate_types(column_validation.clone(), row_iter.clone()).await?;
    Ok(())
}

async fn validate_unique<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    let columns = match column_validation {
        ColumnValidation::All(column_defs) => fetch_all_unique_columns(&column_defs),
        ColumnValidation::SpecifiedColumns(column_defs, specified_columns) => {
            specified_columns_only(fetch_all_unique_columns(&column_defs), &specified_columns)
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

async fn validate_types(
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    let columns = match column_validation {
        ColumnValidation::All(columns) => columns,
        ColumnValidation::SpecifiedColumns(columns, ..) => columns,
    };
    for row in row_iter.clone() {
        for (index, column) in columns.iter().enumerate() {
            if let Some(row_data) = row.get_value(index) {
                if !row_data.is_same_as_data_type(&column.data_type) {
                    return Err(ValidateError::IncompatibleTypeOnTypedField {
                        attempted_value: format!("{:?}", row_data),
                        column_name: column.name.value.to_string(),
                        column_type: column.data_type.to_string(),
                    }
                    .into());
                }
            }
        }
    }
    Ok(())
}
