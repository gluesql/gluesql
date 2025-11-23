mod error;
mod number;
mod text;

pub use error::LiteralError;
pub(crate) use number::{cast_literal_number_to_value, number_literal_to_value};
pub(crate) use text::{cast_literal_text_to_value, text_literal_to_value};
