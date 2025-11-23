mod convert;
mod error;

pub(crate) use convert::{
    cast_literal_number_to_value, cast_literal_text_to_value, number_literal_to_value,
    text_literal_to_value,
};
pub use error::LiteralError;
