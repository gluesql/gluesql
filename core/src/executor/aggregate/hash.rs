use {
    crate::{
        data::{Interval, Value},
        executor::evaluate::Evaluated,
        result::{Error, Result},
    },
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    rust_decimal::Decimal,
    std::fmt::Debug,
};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum GroupKey {
    I8(i8),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U32(u32),
    U64(u64),
    U128(u128),
    Bool(bool),
    Str(String),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Time(NaiveTime),
    Interval(Interval),
    Uuid(u128),
    Decimal(Decimal),
    None,
}

impl TryFrom<&Evaluated<'_>> for GroupKey {
    type Error = Error;

    fn try_from(evaluated: &Evaluated<'_>) -> Result<Self> {
        match evaluated {
            Evaluated::Literal(l) => Value::try_from(l)?.try_into(),
            Evaluated::Value(v) => v.as_ref().try_into(),
        }
    }
}
