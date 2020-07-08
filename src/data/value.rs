use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::Debug;
use thiserror::Error as ThisError;

use sqlparser::ast::{DataType, Value as AstValue};

use crate::result::{Error, Result};

#[derive(ThisError, Debug, PartialEq)]
pub enum ValueError {
    #[error("sql type not supported yet")]
    SqlTypeNotSupported,

    #[error("literal not supported yet")]
    LiteralNotSupported,

    #[error("failed to parse number")]
    FailedToParseNumber,

    #[error("add on non numeric value")]
    AddOnNonNumeric,

    #[error("subtract on non numeric value")]
    SubtractOnNonNumeric,

    #[error("multiply on non numeric value")]
    MultiplyOnNonNumeric,

    #[error("divide on non numeric value")]
    DivideOnNonNumeric,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
}

impl PartialEq<AstValue> for Value {
    fn eq(&self, other: &AstValue) -> bool {
        match (self, other) {
            (Value::Bool(l), AstValue::Boolean(r)) => l == r,
            (Value::I64(l), AstValue::Number(r)) => match r.parse::<i64>() {
                Ok(r) => l == &r,
                Err(_) => false,
            },
            (Value::F64(l), AstValue::Number(r)) => match r.parse::<f64>() {
                Ok(r) => l == &r,
                Err(_) => false,
            },
            (Value::String(l), AstValue::SingleQuotedString(r)) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), Value::I64(r)) => Some(l.cmp(r)),
            (Value::F64(l), Value::F64(r)) => l.partial_cmp(r),
            (Value::String(l), Value::String(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl PartialOrd<AstValue> for Value {
    fn partial_cmp(&self, other: &AstValue) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), AstValue::Number(r)) => match r.parse::<i64>() {
                Ok(r) => Some(l.cmp(&r)),
                Err(_) => None,
            },
            (Value::F64(l), AstValue::Number(r)) => match r.parse::<f64>() {
                Ok(r) => l.partial_cmp(&r),
                Err(_) => None,
            },
            (Value::String(l), AstValue::SingleQuotedString(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl TryFrom<&AstValue> for Value {
    type Error = Error;

    fn try_from(literal: &AstValue) -> Result<Self> {
        match literal {
            AstValue::Number(v) => v
                .parse::<i64>()
                .map_or_else(|_| v.parse::<f64>().map(Value::F64), |v| Ok(Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            AstValue::Boolean(v) => Ok(Value::Bool(*v)),
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
    }
}

impl Value {
    pub fn new(data_type: DataType, literal: &AstValue) -> Result<Self> {
        match (data_type, literal) {
            (DataType::Int, AstValue::Number(v)) => v
                .parse()
                .map(Value::I64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Float(_), AstValue::Number(v)) => v
                .parse()
                .map(Value::F64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Boolean, AstValue::Boolean(v)) => Ok(Value::Bool(*v)),
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
    }

    pub fn clone_by(&self, literal: &AstValue) -> Result<Self> {
        match (self, literal) {
            (Value::I64(_), AstValue::Number(v)) => v
                .parse()
                .map(Value::I64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::F64(_), AstValue::Number(v)) => v
                .parse()
                .map(Value::F64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::String(_), AstValue::SingleQuotedString(v)) => Ok(Value::String(v.clone())),
            (Value::Bool(_), AstValue::Boolean(v)) => Ok(Value::Bool(*v)),
            _ => Err(ValueError::LiteralNotSupported.into()),
        }
    }

    pub fn add(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a + b)),
            (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a + b)),
            _ => Err(ValueError::AddOnNonNumeric.into()),
        }
    }

    pub fn subtract(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a - b)),
            (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a - b)),
            _ => Err(ValueError::SubtractOnNonNumeric.into()),
        }
    }

    pub fn rsubtract(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(b - a)),
            (Value::F64(a), Value::F64(b)) => Ok(Value::F64(b - a)),
            _ => Err(ValueError::SubtractOnNonNumeric.into()),
        }
    }

    pub fn multiply(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a * b)),
            (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a * b)),
            _ => Err(ValueError::MultiplyOnNonNumeric.into()),
        }
    }

    pub fn divide(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a / b)),
            (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a / b)),
            _ => Err(ValueError::DivideOnNonNumeric.into()),
        }
    }

    pub fn rdivide(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(b / a)),
            (Value::F64(a), Value::F64(b)) => Ok(Value::F64(b / a)),
            _ => Err(ValueError::DivideOnNonNumeric.into()),
        }
    }
}
