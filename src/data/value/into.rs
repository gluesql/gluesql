use {
    super::{Value, ValueError},
    crate::result::{Error, Result},
    std::convert::TryInto,
};

impl Into<String> for &Value {
    fn into(self) -> String {
        match self {
            Value::Str(value) => value.to_string(),
            Value::Bool(value) => (if *value { "TRUE" } else { "FALSE" }).to_string(),
            Value::I64(value) => value.to_string(),
            Value::F64(value) => value.to_string(),
            Value::Null => String::from("NULL"),
        }
    }
}

impl TryInto<bool> for &Value {
    type Error = Error;
    fn try_into(self) -> Result<bool> {
        Ok(match self {
            Value::Bool(value) => *value,
            Value::I64(value) => match value {
                1 => true,
                0 => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::F64(value) => {
                if value.eq(&1.0) {
                    true
                } else if value.eq(&0.0) {
                    false
                } else {
                    return Err(ValueError::ImpossibleCast.into());
                }
            }
            Value::Str(value) => match value.to_uppercase().as_str() {
                "TRUE" => true,
                "FALSE" => false,
                _ => return Err(ValueError::ImpossibleCast.into()),
            },
            Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}
impl TryInto<i64> for &Value {
    type Error = Error;
    fn try_into(self) -> Result<i64> {
        Ok(match self {
            Value::Bool(value) => {
                if *value {
                    1
                } else {
                    0
                }
            }
            Value::I64(value) => *value,
            Value::F64(value) => value.trunc() as i64,
            Value::Str(value) => value
                .parse::<i64>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}
impl TryInto<f64> for &Value {
    type Error = Error;
    fn try_into(self) -> Result<f64> {
        Ok(match self {
            Value::Bool(value) => {
                if *value {
                    1.0
                } else {
                    0.0
                }
            }
            Value::I64(value) => (*value as f64).trunc(),
            Value::F64(value) => *value,
            Value::Str(value) => value
                .parse::<f64>()
                .map_err(|_| ValueError::ImpossibleCast)?,
            Value::Null => return Err(ValueError::ImpossibleCast.into()),
        })
    }
}
