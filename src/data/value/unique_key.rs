use {
    super::{Value, ValueError},
    crate::{
        executor::UniqueKey,
        result::{Error, Result},
    },
    std::convert::TryInto,
};

impl TryInto<UniqueKey> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<UniqueKey> {
        use Value::*;

        match self {
            Bool(v) | OptBool(Some(v)) => Ok(UniqueKey::Bool(*v)),
            I64(v) | OptI64(Some(v)) => Ok(UniqueKey::I64(*v)),
            Str(v) | OptStr(Some(v)) => Ok(UniqueKey::Str(v.clone())),
            Empty | OptBool(None) | OptI64(None) | OptStr(None) => Ok(UniqueKey::Null),
            F64(_) | OptF64(_) => Err(ValueError::ConflictOnFloatWithUniqueConstraint.into()),
        }
    }
}
