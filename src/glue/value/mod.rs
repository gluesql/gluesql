use {
    crate::{
        result::{Error, Result},
        Value,
    },
    std::convert::TryInto,
};

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
