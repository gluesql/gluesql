mod unique;

use {
    crate::{data::Row, result::Result, store::Store},
    serde::Serialize,
    sqlparser::ast::{ColumnDef, Ident},
    std::{fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
    unique::{validate_increment, validate_unique},
};

pub use unique::UniqueKey;

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
) -> Result<Option<Vec<Row>>> {
    validate_unique(
        storage,
        table_name,
        column_validation.clone(),
        row_iter.clone(),
    )
    .await?;
    let rows = validate_increment(
        storage,
        table_name,
        column_validation.clone(),
        row_iter.clone(),
    )
    .await?;
    validate_types(column_validation.clone(), row_iter.clone())?;
    Ok(rows)
}

fn validate_types<'a>(
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &'a Row> + Clone,
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
