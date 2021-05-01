use {
    crate::{
        data::Value,
        executor::evaluate::Evaluated,
        result::{Error, Result},
    },
    chrono::NaiveDate,
    std::convert::{TryFrom, TryInto},
};

#[derive(PartialEq, Eq, Hash, Clone, std::fmt::Debug)]
pub enum GroupKey {
    I64(i64),
    Bool(bool),
    Str(String),
    Date(NaiveDate),
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
