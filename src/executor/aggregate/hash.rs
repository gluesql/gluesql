use std::convert::TryFrom;

use crate::executor::evaluate::Evaluated;
use crate::result::{Error, Result};

// Evaluated -> Hash Key
#[derive(PartialEq, Eq, Hash, Clone)]
pub enum GroupKey {
    I64(i64),
    /*
    Bool(bool),
    Str(String),
    */
    Null,
}

impl TryFrom<&Evaluated<'_>> for GroupKey {
    type Error = Error;

    fn try_from(_evaluated: &Evaluated<'_>) -> Result<Self> {
        /*
        match literal {
            AstValue::Number(v) => v
                .parse::<i64>()
                .map_or_else(|_| v.parse::<f64>().map(Value::F64), |v| Ok(Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            AstValue::Boolean(v) => Ok(Value::Bool(*v)),
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
        */

        Ok(GroupKey::I64(1))
    }
}
