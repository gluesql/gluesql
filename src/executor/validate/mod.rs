mod unique;

use {
    serde::Serialize,
    sqlparser::ast::{ColumnDef, Ident},
    std::{fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

pub use unique::{validate_unique, UniqueKey};

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
