use {
    crate::{
        result::{Error, Result},
        Value,
    },
    std::convert::TryInto,
};

impl From<Value> for String {
    fn from(v: Value) -> String {
        (&v).into()
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
