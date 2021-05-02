use {
    super::{error::ValueError, Value},
    crate::{
        executor::GroupKey,
        result::{Error, Result},
    },
    std::convert::TryInto,
};

impl TryInto<GroupKey> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<GroupKey> {
        use Value::*;

        match self {
            Bool(v) => Ok(GroupKey::Bool(*v)),
            I64(v) => Ok(GroupKey::I64(*v)),
            Str(v) => Ok(GroupKey::Str(v.clone())),
            Date(v) => Ok(GroupKey::Date(*v)),
            Timestamp(v) => Ok(GroupKey::Timestamp(*v)),
            Null => Ok(GroupKey::None),
            F64(_) => Err(ValueError::FloatCannotBeGroupedBy.into()),
        }
    }
}

impl TryInto<GroupKey> for Value {
    type Error = Error;

    fn try_into(self) -> Result<GroupKey> {
        use Value::*;

        match self {
            Bool(v) => Ok(GroupKey::Bool(v)),
            I64(v) => Ok(GroupKey::I64(v)),
            Str(v) => Ok(GroupKey::Str(v)),
            Date(v) => Ok(GroupKey::Date(v)),
            Timestamp(v) => Ok(GroupKey::Timestamp(v)),
            Null => Ok(GroupKey::None),
            F64(_) => Err(ValueError::FloatCannotBeGroupedBy.into()),
        }
    }
}
