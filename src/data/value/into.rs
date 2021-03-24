use {
    super::{Value, ValueError},
    crate::result::{Error, Result},
    std::convert::TryInto,
};

macro_rules! impl_into {
    ($type:ident, $($matcher:pat => $result:expr),+) => {
        impl Into<$type> for Value {
            fn into(self) -> $type {
                (&self).into()
            }
        }
        impl Into<$type> for &Value {
            fn into(self) -> $type {
                match self {
                    $($matcher => $result),+
                }
            }
        }
    };
}

macro_rules! impl_try_into {
    ($type:ident, $($matcher:pat => $result:expr),+) => {
        impl TryInto<$type> for Value {
            type Error = Error;
            fn try_into(self) -> Result<$type> {
                (&self).try_into()
            }
        }
        impl TryInto<$type> for &Value {
            type Error = Error;
            fn try_into(self) -> Result<$type> {
                Ok(match self {
                    $($matcher => $result),+
                })
            }
        }
    };
}

impl_into!(
    String,
    Value::Str(value) => value.to_string(),
    Value::Bool(value) => (if *value { "TRUE" } else { "FALSE" }).to_string(),
    Value::I64(value) => value.to_string(),
    Value::F64(value) => value.to_string(),
    Value::Null => String::from("NULL")
);

impl_try_into!(
    bool,
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
    },
    Value::Str(value) => match value.to_uppercase().as_str() {
        "TRUE" => true,
        "FALSE" => false,
        _ => return Err(ValueError::ImpossibleCast.into()),
    },
    Value::Null => return Err(ValueError::ImpossibleCast.into())
);
impl_try_into!(
    i64,
    Value::Bool(value) => {
        if *value {
            1
        } else {
            0
        }
    },
    Value::I64(value) => *value,
    Value::F64(value) => value.trunc() as i64,
    Value::Str(value) => value
        .parse::<i64>()
        .map_err(|_| ValueError::ImpossibleCast)?,
    Value::Null => return Err(ValueError::ImpossibleCast.into())
);
impl_try_into!(
    f64,
    Value::Bool(value) => {
        if *value {
            1.0
        } else {
            0.0
        }
    },
    Value::I64(value) => (*value as f64).trunc(),
    Value::F64(value) => *value,
    Value::Str(value) => value
        .parse::<f64>()
        .map_err(|_| ValueError::ImpossibleCast)?,
    Value::Null => return Err(ValueError::ImpossibleCast.into())
);
