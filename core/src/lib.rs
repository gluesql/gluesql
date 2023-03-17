#![deny(clippy::str_to_string)]

// re-export
pub use {chrono, sqlparser};

mod glue;

pub mod ast;
pub mod ast_builder;
pub mod data;
pub mod executor;
pub mod parse_sql;
pub mod plan;
pub mod result;
pub mod store;
pub mod translate;

pub mod prelude {
    pub use crate::{
        ast::DataType,
        data::{Key, Value},
        executor::{execute, Payload, PayloadVariable},
        glue::Glue,
        parse_sql::parse,
        plan::plan,
        result::Error,
        translate::translate,
    };
}
