mod constraint;
mod fetch;
mod check;

use im_rc::HashSet;
use serde::Serialize;
use sqlparser::ast::{ColumnDef, DataType, ColumnOption, Ident};
use std::{convert::TryInto, fmt::Debug, rc::Rc};
use thiserror::Error as ThisError;

use crate::data::{Row, Value};
use crate::result::Result;
use crate::store::Store;
use crate::utils::Vector;

pub enum ColumnValidation {
    All(Rc<[ColumnDef]>),
    SpecifiedColumns(Rc<[ColumnDef]>, Vec<Ident>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum UniqueKey {
    Bool(bool),
    I64(i64),
    Str(String),
    Null,
}

#[derive(Debug, PartialEq, Serialize, ThisError)]
pub enum ValidateError {
    #[error("conflict! storage row has no column on index {0}")]
    ConflictOnStorageColumnIndex(usize),

    #[error("duplicate entry '{0}' for unique column '{1}'")]
    DuplicateEntryOnUniqueField(String, String),
}

pub async fn validate_rows<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    validate_unique(storage, table_name, column_validation, row_iter).await?;
    validate_type(storage, table_name, column_validation, row_iter).await?;
    Ok(())
}