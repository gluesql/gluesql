use {
    super::{error::ValueError, Value},
    crate::{
        executor::UniqueKey,
        result::{Error, Result},
    },
    std::convert::TryInto,
};

impl TryInto<Option<UniqueKey>> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<Option<UniqueKey>> {
        use Value::*;

        let conflict = |data_type: &str| {
            Err(ValueError::ConflictDataTypeOnUniqueConstraint(data_type.to_owned()).into())
        };

        let unique_key = match self {
            Bool(v) => Some(UniqueKey::Bool(*v)),
            I64(v) => Some(UniqueKey::I64(*v)),
            Str(v) => Some(UniqueKey::Str(v.clone())),
            Date(v) => Some(UniqueKey::Date(*v)),
            Timestamp(v) => Some(UniqueKey::Timestamp(*v)),
            Time(v) => Some(UniqueKey::Time(*v)),
            Interval(v) => Some(UniqueKey::Interval(*v)),
            Uuid(v) => Some(UniqueKey::Uuid(*v)),
            Null => None,
            F64(_) => return conflict("FLOAT"),
            Map(_) => return conflict("MAP"),
            List(_) => return conflict("LIST"),
        };

        Ok(unique_key)
    }
}
