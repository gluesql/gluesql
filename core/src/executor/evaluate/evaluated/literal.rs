mod convert;
mod error;

pub(crate) use convert::{
    number_literal_to_value, text_literal_to_value, try_cast_literal_to_value,
};
pub use error::LiteralError;
