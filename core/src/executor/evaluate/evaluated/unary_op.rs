use {
    super::Evaluated,
    crate::{data::Value, executor::evaluate::error::EvaluateError, result::Result},
    std::borrow::Cow,
};

impl<'a> Evaluated<'a> {
    pub fn unary_plus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Number(_) => Ok(self.clone()),
            Evaluated::Text(text) => {
                Err(EvaluateError::UnsupportedUnaryPlus(text.to_string()).into())
            }
            Evaluated::Value(v) => v.unary_plus().map(Evaluated::Value),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::UnsupportedUnaryPlus(source[range.clone()].to_owned()).into())
            }
        }
    }

    pub fn unary_minus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Number(value) => Ok(Evaluated::Number(Cow::Owned(-value.as_ref()))),
            Evaluated::Text(text) => {
                Err(EvaluateError::UnsupportedUnaryMinus(text.to_string()).into())
            }
            Evaluated::Value(v) => v.unary_minus().map(Evaluated::Value),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::UnsupportedUnaryMinus(source[range.clone()].to_owned()).into())
            }
        }
    }

    pub fn unary_not(self) -> Result<Evaluated<'a>> {
        if self.is_null() {
            Ok(self)
        } else {
            self.try_into()
                .map(|v: bool| Evaluated::Value(Value::Bool(!v)))
        }
    }

    pub fn unary_factorial(&self) -> Result<Evaluated<'a>> {
        match self {
            literal @ (Evaluated::Number(_) | Evaluated::Text(_)) => {
                Value::try_from(literal.clone()).and_then(|v| v.unary_factorial())
            }
            Evaluated::Value(v) => v.unary_factorial(),
            Evaluated::StrSlice { source, range } => Err(EvaluateError::UnsupportedUnaryFactorial(
                source[range.clone()].to_owned(),
            )
            .into()),
        }
        .map(Evaluated::Value)
    }

    pub fn unary_bitwise_not(&self) -> Result<Evaluated<'a>> {
        match self {
            literal @ (Evaluated::Number(_) | Evaluated::Text(_)) => {
                Value::try_from(literal.clone()).and_then(|v| v.unary_bitwise_not())
            }
            Evaluated::Value(v) => v.unary_bitwise_not(),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::IncompatibleUnaryBitwiseNotOperation(
                    source[range.clone()].to_owned(),
                )
                .into())
            }
        }
        .map(Evaluated::Value)
    }
}
