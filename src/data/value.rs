use nom_sql::{Literal, SqlType};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Debug;
use thiserror::Error;

use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum ValueError {
    #[error("sql type not supported yet")]
    SqlTypeNotSupported,

    #[error("literal not supported yet")]
    LiteralNotSupported,

    #[error("cannot run {0:?} {1:?}")]
    AddOnNonNumeric(Value, Value),
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
            _ => Err(ValueError::AddOnNonNumeric(self.clone(), other.clone()).into()),
        }
    }
}
