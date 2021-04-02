use {
    crate::{
        result::{Error, Result},
        Value,
    },
    std::convert::TryInto,
};

impl From<Value> for String {
    fn from(value: Value) -> String {
        (&value).into()
    }
}

impl TryInto<bool> for Value {
    type Error = Error;
    fn try_into(self) -> Result<bool> {
        (&self).try_into()
    }
}
impl TryInto<i64> for Value {
    type Error = Error;
    fn try_into(self) -> Result<i64> {
        (&self).try_into()
    }
}
impl TryInto<f64> for Value {
    type Error = Error;
    fn try_into(self) -> Result<f64> {
        (&self).try_into()
    }
}

impl From<Value> for serde_json::value::Value {
    fn from(value: Value) -> serde_json::value::Value {
        match value {
            Value::Bool(value) => value.into(),
            Value::I64(value) => value.into(),
            Value::F64(value) => value.into(),
            Value::Str(value) => value.into(),
            Value::Null => serde_json::value::Value::Null,
        }
    }
}
