use std::convert::TryFrom;

use crate::data::Value;
use crate::executor::evaluate::Evaluated;
use crate::result::{Error, Result};

#[derive(PartialEq, Eq, Hash, Clone, std::fmt::Debug)]
pub enum GroupKey {
    I64(i64),
    Bool(bool),
    Str(String),
    Null,
}

impl TryFrom<&Evaluated<'_>> for GroupKey {
    type Error = Error;

    fn try_from(evaluated: &Evaluated<'_>) -> Result<Self> {
        match evaluated {
            Evaluated::LiteralRef(l) => GroupKey::try_from(&Value::try_from(*l)?),
            Evaluated::Literal(l) => GroupKey::try_from(&Value::try_from(l)?),
            Evaluated::ValueRef(v) => GroupKey::try_from(*v),
            Evaluated::Value(v) => GroupKey::try_from(v),
            Evaluated::StringRef(s) => Ok(GroupKey::Str(s.to_string())),
        }
    }
}

impl TryFrom<&Value> for GroupKey {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self> {
        use Value::*;

        match value {
            Bool(v) | OptBool(Some(v)) => Ok(GroupKey::Bool(*v)),
            I64(v) | OptI64(Some(v)) => Ok(GroupKey::I64(*v)),
            Str(v) | OptStr(Some(v)) => Ok(GroupKey::Str(v.clone())),
            Empty | OptBool(None) | OptI64(None) | OptStr(None) => Ok(GroupKey::Null),
            _ => {
                panic!();
            }
        }
    }
}
