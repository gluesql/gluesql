use {
    crate::{
        data::Value,
        executor::Evaluated,
        result::{Error, Result},
    },
    std::convert::TryFrom,
};

impl TryFrom<Evaluated<'_>> for Value {
    type Error = Error;

    fn try_from(evaluated: Evaluated) -> Result<Self> {
        match evaluated {
            Evaluated::Literal(literal) => Value::try_from(literal),
            Evaluated::Value(value) => Ok(value),
        }
    }
}
