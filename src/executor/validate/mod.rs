mod constraint;
mod fetch;
mod check;
mod confirm_types;

use {
	std::{
		fmt::Debug,
		rc::Rc,
	},
	sqlparser::ast::{
      ColumnDef,
      Ident
  },
	crate::{
		data::Row,
		result::Result,
		store::Store,
	},
	check::{
		validate_unique,
	},
	confirm_types::confirm_types,
	serde::Serialize,

    thiserror::Error as ThisError,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum UniqueKey {
    Bool(bool),
    I64(i64),
    Str(String),
    Null,
}

#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum ValidateError {
    #[error("conflict! storage row has no column on index {0}")]
    ConflictOnStorageColumnIndex(usize),

    #[error("duplicate entry '{0}' for unique column '{1}'")]
    DuplicateEntryOnUniqueField(String, String),

    #[error("incompatible type {0} used for typed column '{1}' ({2})")]
    IncompatibleTypeOnTypedField(String, String, String),
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
    validate_unique(storage, table_name, column_validation.clone(), row_iter.clone()).await?;
    confirm_types(storage, table_name, column_validation.clone(), row_iter.clone()).await?;
    Ok(())
}