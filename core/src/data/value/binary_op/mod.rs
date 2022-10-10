use crate::{prelude::Value, result::Result};

mod decimal;
mod f64;
mod i128;
mod i16;
mod i32;
mod i64;
mod integer;

pub trait TryBinaryOperator {
    type Rhs;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value>;
}
