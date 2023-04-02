mod alter_table;
mod error;
mod index;
mod table;
mod validate;

use validate::{validate, validate_column_names};

pub use {
    alter_table::alter_table,
    error::AlterError,
    index::create_index,
    table::{create_table, drop_table},
};
