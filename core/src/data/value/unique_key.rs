use {
    super::{error::ValueError, Value},
    crate::{
        executor::UniqueKey,
        result::{Error, Result},
    },
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
            I8(v) => Some(UniqueKey::I8(*v)),
            I32(v) => Some(UniqueKey::I32(*v)),
            I64(v) => Some(UniqueKey::I64(*v)),
            I128(v) => Some(UniqueKey::I128(*v)),
            U8(v) => Some(UniqueKey::U8(*v)),
            U32(v) => Some(UniqueKey::U32(*v)),
            U64(v) => Some(UniqueKey::U64(*v)),
            U128(v) => Some(UniqueKey::U128(*v)),
            Str(v) => Some(UniqueKey::Str(v.clone())),
            Date(v) => Some(UniqueKey::Date(*v)),
            Timestamp(v) => Some(UniqueKey::Timestamp(*v)),
            Time(v) => Some(UniqueKey::Time(*v)),
            Interval(v) => Some(UniqueKey::Interval(*v)),
            Uuid(v) => Some(UniqueKey::Uuid(*v)),
            Decimal(v) => Some(UniqueKey::Decimal(*v)),
            Null => None,
            F64(_) => return conflict("FLOAT"),
            Map(_) => return conflict("MAP"),
            List(_) => return conflict("LIST"),
        };

        Ok(unique_key)
    }
}
