use {
    super::{Value, ValueError},
    crate::result::Result,
};

impl Into<String> for Value {
    fn into(self) -> String {
        match self {
            Value::Str(value) => value.into(),
            Value::Bool(value) => (if value { "TRUE" } else { "FALSE" }).to_string(),
            Value::I64(value) => value.to_string(),
            Value::F64(value) => value.to_string(),
            Value::Null => String::new(), // Hmmm...
        }
    }
}

pub trait TryInto<T>: Sized {
    fn try_into(&self) -> Result<T>;
}

impl TryInto<bool> for Value {
    fn try_into(&self) -> Result<bool> {
        Ok(match self {
            Value::Bool(value) => *value,
            Value::I64(value) => match value {
                1 => true,
                0 => false,
                _ => Err(ValueError::ImpossibleCast)?,
            },
            Value::F64(value) => {
                if value.eq(&1.0) {
                    true
                } else if value.eq(&0.0) {
                    false
                } else {
                    Err(ValueError::ImpossibleCast)?
                }
            }
            Value::Str(value) => match value.to_uppercase().as_str() {
                "TRUE" => true,
                "FALSE" => false,
                _ => Err(ValueError::ImpossibleCast)?,
            },
            Value::Null => Err(ValueError::ImpossibleCast)?,
        })
    }
}
impl TryInto<i64> for Value {
    fn try_into(&self) -> Result<i64> {
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
            Value::Null => Err(ValueError::ImpossibleCast)?,
        })
    }
}
impl TryInto<f64> for Value {
    fn try_into(&self) -> Result<f64> {
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
            Value::Null => Err(ValueError::ImpossibleCast)?,
        })
    }
}
impl TryInto<String> for Value {
    fn try_into(&self) -> Result<String> {
        Ok(self.to_owned().into())
    }
}
