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

        let unique_key = match self {
            Bool(v) => Some(UniqueKey::Bool(*v)),
            I64(v) => Some(UniqueKey::I64(*v)),
            Str(v) => Some(UniqueKey::Str(v.clone())),
            Null => None,
            F64(_) => {
                return Err(ValueError::ConflictOnFloatWithUniqueConstraint.into());
            }
        };

        Ok(unique_key)
    }
}
