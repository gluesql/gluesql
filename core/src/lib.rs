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
        data::{Key, Value},
        executor::{execute, Payload, PayloadVariable},
        glue::Glue,
        parse_sql::parse,
        plan::plan,
        result::{Error, Result},
        translate::translate,
    };
}

pub mod error {
    pub use crate::result::*;
}

#[cfg(not(feature = "send"))]
pub(crate) use std::rc::Rc as Grc;
#[cfg(feature = "send")]
pub(crate) use std::sync::Arc as Grc;

#[cfg(feature = "send")]
pub(crate) use im::{HashMap, HashSet};
#[cfg(not(feature = "send"))]
pub(crate) use im_rc::{HashMap, HashSet};
