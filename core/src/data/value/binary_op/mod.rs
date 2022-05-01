use crate::{prelude::Value, result::Result};

mod decimal;
mod f64;
mod i64;
mod i8;

pub trait TryBinaryOperator {
    type Rhs;

    fn try_add(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value>;
    fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value>;
}
