mod alter_table;
mod error;
mod function;
mod index;
mod table;
mod validate;
mod trigger;

use validate::{validate, validate_arg_names, validate_column_names, validate_default_args};

pub use {
    alter_table::alter_table,
    trigger::{delete_trigger, insert_trigger},
    error::AlterError,
    function::{delete_function, insert_function},
    index::create_index,
    table::{create_table, drop_table, CreateTableOptions, Referencing},
};
