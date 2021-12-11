use {
    super::{error::ValueError, Value},
    crate::{
        executor::GroupKey,
        result::{Error, Result},
    },
};

impl TryInto<GroupKey> for Value {
    type Error = Error;

    fn try_into(self) -> Result<GroupKey> {
        use Value::*;

        match self {
            Bool(v) => Ok(GroupKey::Bool(v)),
            I8(v) => Ok(GroupKey::I8(v)),
            I64(v) => Ok(GroupKey::I64(v)),
            Str(v) => Ok(GroupKey::Str(v)),
            Date(v) => Ok(GroupKey::Date(v)),
            Timestamp(v) => Ok(GroupKey::Timestamp(v)),
            Time(v) => Ok(GroupKey::Time(v)),
            Interval(v) => Ok(GroupKey::Interval(v)),
            Uuid(v) => Ok(GroupKey::Uuid(v)),
            Decimal(v) => Ok(GroupKey::Decimal(v)),
            Null => Ok(GroupKey::None),
            F64(_) => Err(ValueError::GroupByNotSupported("FLOAT".to_owned()).into()),
            Map(_) => Err(ValueError::GroupByNotSupported("MAP".to_owned()).into()),
            List(_) => Err(ValueError::GroupByNotSupported("LIST".to_owned()).into()),
        }
    }
}

impl TryInto<GroupKey> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<GroupKey> {
        self.clone().try_into()
    }
}
