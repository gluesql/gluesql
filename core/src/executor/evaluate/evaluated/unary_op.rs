use {
    super::Evaluated,
    crate::{
        data::Value,
        executor::evaluate::{error::EvaluateError, literal::Literal},
        result::Result,
    },
    std::borrow::Cow,
};

impl<'a> Evaluated<'a> {
    pub fn unary_plus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(literal @ Literal::Number(_)) => {
                Ok(Evaluated::Literal(literal.clone()))
            }
            Evaluated::Literal(literal) => {
                Err(EvaluateError::UnsupportedUnaryPlus(literal.to_string()).into())
            }
            Evaluated::Value(v) => v.unary_plus().map(Evaluated::Value),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::UnsupportedUnaryPlus(source[range.clone()].to_owned()).into())
            }
        }
    }

    pub fn unary_minus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(Literal::Number(value)) => Ok(Evaluated::Literal(Literal::Number(
                Cow::Owned(-value.as_ref()),
            ))),
            Evaluated::Literal(literal) => {
                Err(EvaluateError::UnsupportedUnaryMinus(literal.to_string()).into())
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
            Evaluated::Literal(v) => Value::try_from(v).and_then(|v| v.unary_factorial()),
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
            Evaluated::Literal(v) => Value::try_from(v).and_then(|v| v.unary_bitwise_not()),
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
