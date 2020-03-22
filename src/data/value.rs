use nom_sql::{Literal, SqlType};
use serde::{Deserialize, Serialize};
use std::convert::From;
use std::fmt::Debug;

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
            _ => unimplemented!(),
        }
    }
}

impl From<(SqlType, Literal)> for Value {
    fn from((sql_type, literal): (SqlType, Literal)) -> Self {
        match (sql_type, literal) {
            (SqlType::Int(_), Literal::Integer(v)) => Value::I64(v),
            (SqlType::Text, Literal::String(v)) => Value::String(v),
            _ => unimplemented!(),
        }
    }
}

impl From<(Value, &Literal)> for Value {
    fn from((value, literal): (Value, &Literal)) -> Self {
        match (value, literal) {
            (Value::I64(_), &Literal::Integer(v)) => Value::I64(v),
            (Value::String(_), &Literal::String(ref v)) => Value::String(v.clone()),
            _ => unimplemented!(),
        }
    }
}
