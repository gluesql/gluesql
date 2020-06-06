use nom_sql::{Literal, SqlType};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::{DataType, Value as AstValue};

use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum ValueError {
    #[error("sql type not supported yet")]
    SqlTypeNotSupported,

    #[error("literal not supported yet")]
    LiteralNotSupported,

    #[error("add on non numeric value")]
    AddOnNonNumeric,

    #[error("subtract on non numeric value")]
    SubtractOnNonNumeric,

    #[error("multiply on non numeric value")]
    MultiplyOnNonNumeric,

    #[error("divide on non numeric value")]
    DivideOnNonNumeric,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    I64(i64),
    String(String),
}

impl PartialEq<Literal> for Value {
    fn eq(&self, other: &Literal) -> bool {
        match (self, other) {
            (Value::I64(l), Literal::Integer(r)) => l == r,
            (Value::String(l), Literal::String(r)) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), Value::I64(r)) => Some(l.cmp(r)),
            (Value::String(l), Value::String(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl PartialOrd<Literal> for Value {
    fn partial_cmp(&self, other: &Literal) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), Literal::Integer(r)) => Some(l.cmp(r)),
            (Value::String(l), Literal::String(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl Value {
    pub fn new(sql_type: SqlType, literal: Literal) -> Result<Self> {
        match (sql_type, literal) {
            (SqlType::Int(_), Literal::Integer(v)) => Ok(Value::I64(v)),
            (SqlType::Text, Literal::String(v)) => Ok(Value::String(v)),
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
    }

    pub fn new2(data_type: DataType, literal: &AstValue) -> Result<Self> {
        match (data_type, literal) {
            (DataType::Int, AstValue::Number(v)) => v
                .parse()
                .map_or_else(|_| unimplemented!(), |v| Ok(Value::I64(v))),
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
    }

    pub fn clone_by(&self, literal: &Literal) -> Result<Self> {
        match (self, literal) {
            (Value::I64(_), &Literal::Integer(v)) => Ok(Value::I64(v)),
            (Value::String(_), &Literal::String(ref v)) => Ok(Value::String(v.clone())),
            _ => Err(ValueError::LiteralNotSupported.into()),
        }
    }

    pub fn add(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a + b)),
            _ => Err(ValueError::AddOnNonNumeric.into()),
        }
    }

    pub fn subtract(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a - b)),
            _ => Err(ValueError::SubtractOnNonNumeric.into()),
        }
    }

    pub fn rsubtract(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(b - a)),
            _ => Err(ValueError::SubtractOnNonNumeric.into()),
        }
    }

    pub fn multiply(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a * b)),
            _ => Err(ValueError::MultiplyOnNonNumeric.into()),
        }
    }

    pub fn divide(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a / b)),
            _ => Err(ValueError::DivideOnNonNumeric.into()),
        }
    }

    pub fn rdivide(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(b / a)),
            _ => Err(ValueError::DivideOnNonNumeric.into()),
        }
    }
}
