use {
    crate::{
        data::{Interval, Value},
        executor::evaluate::Evaluated,
        result::{Error, Result},
    },
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    std::{
        convert::{TryFrom, TryInto},
        fmt::Debug,
    },
    rust_decimal::Decimal,
};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum GroupKey {
    I64(i64),
    Bool(bool),
    Str(String),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Time(NaiveTime),
    Interval(Interval),
    UUID(u128),
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
