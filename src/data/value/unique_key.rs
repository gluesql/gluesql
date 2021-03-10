use {
    super::{error::ValueError, Value},
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
            Bool(v) => Ok(UniqueKey::Bool(*v)),
            I64(v) => Ok(UniqueKey::I64(*v)),
            Str(v) => Ok(UniqueKey::Str(v.clone())),
            Null => Ok(UniqueKey::Null),
            F64(_) => Err(ValueError::ConflictOnFloatWithUniqueConstraint.into()),
        }
    }
}
