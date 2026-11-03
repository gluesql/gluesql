mod alter_table;
mod error;
mod function;
mod index;
mod table;
mod validate;

use validate::{validate, validate_column_def, validate_column_names};

pub use {
    alter_table::alter_table,
    error::AlterError,
    function::{delete_function, insert_function},
    index::create_index,
    table::{CreateTableOptions, Referencing, create_table, drop_table},
};
