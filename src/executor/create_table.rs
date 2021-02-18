use serde::Serialize;
use sqlparser::ast::{ColumnDef, ColumnOption, DataType};
use std::fmt::Debug;
use thiserror::Error as ThisError;

use crate::data::Schema;
use crate::result::Result;
use crate::store::Store;

#[derive(Debug, PartialEq, Serialize, ThisError)]
pub enum CreateTableError {
    #[error("table already exists")]
    TableAlreadyExists,

    #[error("column '{0}' of data type '{1}' is unsupported for unique constraint")]
    UnsupportedDataTypeForUniqueColumn(String, String),
}

pub async fn validate_table<T: 'static + Debug>(
    storage: &impl Store<T>,
    schema: &Schema,
    if_not_exists: bool,
) -> Result<()> {
    validate_column_unique_option(&schema.column_defs)?;
    validate_table_if_not_exists(storage, &schema.table_name, if_not_exists).await
}

fn validate_column_unique_option(column_defs: &[ColumnDef]) -> Result<()> {
    let found = column_defs.iter().find(|col| match col.data_type {
        DataType::Float(_) => col
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::Unique { .. })),
        _ => false,
    });

    if let Some(col) = found {
        return Err(CreateTableError::UnsupportedDataTypeForUniqueColumn(
            col.name.to_string(),
            col.data_type.to_string(),
        )
        .into());
    }

    Ok(())
}

async fn validate_table_if_not_exists<'a, T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    if_not_exists: bool,
) -> Result<()> {
    if if_not_exists || storage.fetch_schema(table_name).await?.is_none() {
        return Ok(());
    }

    Err(CreateTableError::TableAlreadyExists.into())
}
