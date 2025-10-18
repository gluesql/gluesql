#![deny(clippy::str_to_string)]

// re-export
pub use {chrono, sqlparser};

mod glue;
mod mock;
mod result;

pub mod ast;
pub mod ast_builder;
pub mod data;
pub mod executor;
pub mod parse_sql;
pub mod plan;
pub mod row_conversion;
pub mod store;
pub mod translate;

pub mod prelude {
    pub use crate::{
        ast::DataType,
        data::{Key, Value},
        executor::{Payload, PayloadVariable, execute},
        glue::Glue,
        parse_sql::parse,
        result::{Error, Result},
        row_conversion::{SelectExt, SelectResultExt},
        translate::translate,
    };
}

pub mod error {
    pub use crate::result::*;
}
