use {
    super::Evaluated,
    crate::{
        ast::DataType,
        data::{BigDecimalExt, Value},
        executor::evaluate::error::EvaluateError,
        result::Result,
    },
    std::borrow::Cow,
};

impl<'a> Evaluated<'a> {
    pub fn unary_plus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Number(value) => Ok(Evaluated::Number(value.clone())),
            Evaluated::Text(text) => {
                Err(EvaluateError::UnsupportedUnaryPlus(text.to_string()).into())
            }
            Evaluated::Value(v) => v
                .as_ref()
                .unary_plus()
                .map(|v| Evaluated::Value(Cow::Owned(v))),
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
            Evaluated::Value(v) => v
                .as_ref()
                .unary_minus()
                .map(|v| Evaluated::Value(Cow::Owned(v))),
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
                .map(|v: bool| Evaluated::Value(Cow::Owned(Value::Bool(!v))))
        }
    }

    pub fn unary_factorial(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Number(decimal) => decimal
                .to_i64()
                .map(Value::I64)
                .ok_or(
                    EvaluateError::NumberParseFailed {
                        literal: decimal.to_string(),
                        data_type: DataType::Int,
                    }
                    .into(),
                )
                .and_then(|v| v.unary_factorial()),
            Evaluated::Text(text) => {
                Err(EvaluateError::UnaryFactorialRequiresNumericLiteral(text.to_string()).into())
            }
            Evaluated::Value(v) => v.as_ref().unary_factorial(),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::UnaryFactorialRequiresNumericLiteral(
                    source[range.clone()].to_owned(),
                )
                .into())
            }
        }
        .map(|v| Evaluated::Value(Cow::Owned(v)))
    }

    pub fn unary_bitwise_not(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Number(decimal) => decimal
                .to_i64()
                .map(Value::I64)
                .ok_or(
                    EvaluateError::NumberParseFailed {
                        literal: decimal.to_string(),
                        data_type: DataType::Int,
                    }
                    .into(),
                )
                .and_then(|v| v.unary_bitwise_not()),
            Evaluated::Text(text) => {
                Err(EvaluateError::UnaryBitwiseNotRequiresIntegerLiteral(text.to_string()).into())
            }
            Evaluated::Value(v) => v.as_ref().unary_bitwise_not(),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::UnaryBitwiseNotRequiresIntegerLiteral(
                    source[range.clone()].to_owned(),
                )
                .into())
            }
        }
        .map(|v| Evaluated::Value(Cow::Owned(v)))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::data::{Value, ValueError},
        bigdecimal::BigDecimal,
        std::{borrow::Cow, str::FromStr},
    };

    fn number(value: i64) -> Evaluated<'static> {
        Evaluated::Number(Cow::Owned(BigDecimal::from(value)))
    }

    fn text(value: &'static str) -> Evaluated<'static> {
        Evaluated::Text(Cow::Borrowed(value))
    }

    fn str_slice(value: &'static str, range: std::ops::Range<usize>) -> Evaluated<'static> {
        Evaluated::StrSlice {
            source: Cow::Borrowed(value),
            range,
        }
    }

    #[test]
    fn unary_plus() {
        assert_eq!(number(5).unary_plus(), Ok(number(5)));
        assert_eq!(
            Evaluated::Value(Cow::Owned(Value::I64(3))).unary_plus(),
            Ok(Evaluated::Value(Cow::Owned(Value::I64(3))))
        );
        assert_eq!(
            Evaluated::Value(Cow::Owned(Value::Null)).unary_plus(),
            Ok(Evaluated::Value(Cow::Owned(Value::Null)))
        );
        assert_eq!(
            text("abc").unary_plus(),
            Err(EvaluateError::UnsupportedUnaryPlus("abc".to_owned()).into())
        );
        assert_eq!(
            str_slice("abc", 0..2).unary_plus(),
            Err(EvaluateError::UnsupportedUnaryPlus("ab".to_owned()).into())
        );
    }

    #[test]
    fn unary_minus() {
        assert_eq!(number(7).unary_minus(), Ok(number(-7)));
        assert_eq!(
            text("abc").unary_minus(),
            Err(EvaluateError::UnsupportedUnaryMinus("abc".to_owned()).into())
        );
        assert_eq!(
            str_slice("abc", 0..2).unary_minus(),
            Err(EvaluateError::UnsupportedUnaryMinus("ab".to_owned()).into())
        );
    }

    #[test]
    fn unary_not() {
        assert_eq!(
            Evaluated::Value(Cow::Owned(Value::Bool(true))).unary_not(),
            Ok(Evaluated::Value(Cow::Owned(Value::Bool(false))))
        );
        let null = Evaluated::Value(Cow::Owned(Value::Null));
        assert_eq!(null.clone().unary_not(), Ok(null));
        assert_eq!(
            number(1).unary_not(),
            Err(EvaluateError::BooleanTypeRequired("1".to_owned()).into())
        );
    }

    #[test]
    fn unary_factorial() {
        assert_eq!(
            number(5).unary_factorial(),
            Ok(Evaluated::Value(Cow::Owned(Value::I128(120))))
        );
        assert_eq!(
            Evaluated::Value(Cow::Owned(Value::I64(-1))).unary_factorial(),
            Err(ValueError::FactorialOnNegativeNumeric.into())
        );
        assert_eq!(
            Evaluated::Number(Cow::Owned(BigDecimal::from_str("5.5").unwrap())).unary_factorial(),
            Err(EvaluateError::NumberParseFailed {
                literal: "5.5".to_owned(),
                data_type: DataType::Int
            }
            .into())
        );
        assert_eq!(
            text("abc").unary_factorial(),
            Err(EvaluateError::UnaryFactorialRequiresNumericLiteral("abc".to_owned()).into())
        );
        assert_eq!(
            str_slice("abcd", 1..3).unary_factorial(),
            Err(EvaluateError::UnaryFactorialRequiresNumericLiteral("bc".to_owned()).into())
        );
    }

    #[test]
    fn unary_bitwise_not() {
        assert_eq!(
            number(5).unary_bitwise_not(),
            Ok(Evaluated::Value(Cow::Owned(Value::I64(!5))))
        );
        assert_eq!(
            Evaluated::Value(Cow::Owned(Value::F64(1.5))).unary_bitwise_not(),
            Err(ValueError::UnaryBitwiseNotOnNonInteger.into())
        );
        assert_eq!(
            Evaluated::Number(Cow::Owned(BigDecimal::from_str("5.5").unwrap())).unary_bitwise_not(),
            Err(EvaluateError::NumberParseFailed {
                literal: "5.5".to_owned(),
                data_type: DataType::Int
            }
            .into())
        );
        assert_eq!(
            text("abc").unary_bitwise_not(),
            Err(EvaluateError::UnaryBitwiseNotRequiresIntegerLiteral("abc".to_owned()).into())
        );
        assert_eq!(
            str_slice("abcd", 1..3).unary_bitwise_not(),
            Err(EvaluateError::UnaryBitwiseNotRequiresIntegerLiteral("bc".to_owned()).into())
        );
    }
}
