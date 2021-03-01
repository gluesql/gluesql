use {
    super::{Value, ValueError},
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
            Bool(v) | OptBool(Some(v)) => Ok(GroupKey::Bool(*v)),
            I64(v) | OptI64(Some(v)) => Ok(GroupKey::I64(*v)),
            Str(v) | OptStr(Some(v)) => Ok(GroupKey::Str(v.clone())),
            Empty | OptBool(None) | OptI64(None) | OptStr(None) => Ok(GroupKey::Null),
            F64(_) | OptF64(_) => Err(ValueError::FloatCannotBeGroupedBy.into()),
        }
    }
}

impl TryInto<GroupKey> for Value {
    type Error = Error;

    fn try_into(self) -> Result<GroupKey> {
        use Value::*;

        match self {
            Bool(v) | OptBool(Some(v)) => Ok(GroupKey::Bool(v)),
            I64(v) | OptI64(Some(v)) => Ok(GroupKey::I64(v)),
            Str(v) | OptStr(Some(v)) => Ok(GroupKey::Str(v)),
            Empty | OptBool(None) | OptI64(None) | OptStr(None) => Ok(GroupKey::Null),
            F64(_) | OptF64(_) => Err(ValueError::FloatCannotBeGroupedBy.into()),
        }
    }
}
