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
pub mod store;
pub mod translate;

pub mod prelude {
    pub use crate::{
        ast::DataType,
        ast_builder::table,
        data::{Key, Value},
        executor::{execute, Payload, PayloadVariable},
        glue::Glue,
        parse_sql::parse,
        plan::plan,
        result::{Error, Result},
        translate::translate,
    };
    pub mod exprs {
        pub use crate::ast_builder::{
            bitwise_not, case, col, date, exists, expr, factorial, minus, nested, not, not_exists,
            null, num, plus, subquery, text, time, timestamp,
        };
    }
    pub mod func {
        pub use crate::ast_builder::{avg, count, function, max, min, stdev, sum, variance};
    }
    pub mod transaction {
        pub use crate::ast_builder::{begin, commit, rollback};
    }
}

pub mod error {
    pub use crate::result::*;
}
