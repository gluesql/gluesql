use nom_sql::{Literal, SqlType};
use serde::{Deserialize, Serialize};
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

impl Value {
    pub fn new(sql_type: SqlType, literal: Literal) -> Self {
        match (sql_type, literal) {
            (SqlType::Int(_), Literal::Integer(v)) => Value::I64(v),
            (SqlType::Text, Literal::String(v)) => Value::String(v),
            _ => unimplemented!(),
        }
    }

    pub fn clone_by(&self, literal: &Literal) -> Self {
        match (self, literal) {
            (Value::I64(_), &Literal::Integer(v)) => Value::I64(v),
            (Value::String(_), &Literal::String(ref v)) => Value::String(v.clone()),
            _ => unimplemented!(),
        }
    }
}
