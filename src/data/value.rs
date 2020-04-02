use nom_sql::{Literal, SqlType};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum ValueError {
    #[error("sql type not supported yet")]
    SqlTypeNotSupported,

    #[error("literal not supported yet")]
    LiteralNotSupported,
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
}
