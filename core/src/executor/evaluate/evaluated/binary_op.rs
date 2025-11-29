use {
    super::Evaluated,
    crate::{
        ast::BinaryOperator,
        data::{BigDecimalExt, Value},
        executor::evaluate::error::EvaluateError,
        result::{Error, Result},
    },
    bigdecimal::BigDecimal,
    std::borrow::Cow,
};

impl<'a> Evaluated<'a> {
    pub fn add<'b, 'c>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'c>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            return Ok(Evaluated::Number(Cow::Owned(l.as_ref() + r.as_ref())));
        }

        binary_op(self, other, BinaryOperator::Plus, Value::add)
    }

    pub fn subtract<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            return Ok(Evaluated::Number(Cow::Owned(l.as_ref() - r.as_ref())));
        }

        binary_op(self, other, BinaryOperator::Minus, Value::subtract)
    }

    pub fn multiply<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            return Ok(Evaluated::Number(Cow::Owned(l.as_ref() * r.as_ref())));
        }

        binary_op(self, other, BinaryOperator::Multiply, Value::multiply)
    }

    pub fn divide<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            if *r.as_ref() == BigDecimal::from(0) {
                return Err(EvaluateError::DivisorShouldNotBeZero.into());
            }

            return Ok(Evaluated::Number(Cow::Owned(l.as_ref() / r.as_ref())));
        }

        binary_op(self, other, BinaryOperator::Divide, Value::divide)
    }

    pub fn modulo<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            if *r.as_ref() == BigDecimal::from(0) {
                return Err(EvaluateError::DivisorShouldNotBeZero.into());
            }

            return Ok(Evaluated::Number(Cow::Owned(l.as_ref() % r.as_ref())));
        }

        binary_op(self, other, BinaryOperator::Modulo, Value::modulo)
    }

    pub fn bitwise_and<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            let lhs = l.to_i64().ok_or_else(|| {
                unsupported_literal_binary_op(self, BinaryOperator::BitwiseAnd, other)
            })?;
            let rhs = r.to_i64().ok_or_else(|| {
                unsupported_literal_binary_op(self, BinaryOperator::BitwiseAnd, other)
            })?;

            return Ok(Evaluated::Number(Cow::Owned(BigDecimal::from(lhs & rhs))));
        } else if is_literal(self) && is_literal(other) {
            return Err(unsupported_literal_binary_op(
                self,
                BinaryOperator::BitwiseAnd,
                other,
            ));
        }

        binary_op(self, other, BinaryOperator::BitwiseAnd, Value::bitwise_and)
    }

    pub fn bitwise_shift_left<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            let lhs = l
                .to_i64()
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            if !r.is_integer_representation() {
                return Err(incompatible_bit_operation(self, other));
            }

            let rhs = r
                .to_u32()
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            let result = lhs
                .checked_shl(rhs)
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            return Ok(Evaluated::Number(Cow::Owned(BigDecimal::from(result))));
        } else if is_literal(self) && is_literal(other) {
            return Err(unsupported_literal_binary_op(
                self,
                BinaryOperator::BitwiseShiftLeft,
                other,
            ));
        }

        binary_op(
            self,
            other,
            BinaryOperator::BitwiseShiftLeft,
            Value::bitwise_shift_left,
        )
    }

    pub fn bitwise_shift_right<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            let lhs = l
                .to_i64()
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            if !r.is_integer_representation() {
                return Err(incompatible_bit_operation(self, other));
            }

            let rhs = r
                .to_u32()
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            let result = lhs
                .checked_shr(rhs)
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            return Ok(Evaluated::Number(Cow::Owned(BigDecimal::from(result))));
        } else if is_literal(self) && is_literal(other) {
            return Err(unsupported_literal_binary_op(
                self,
                BinaryOperator::BitwiseShiftRight,
                other,
            ));
        }

        binary_op(
            self,
            other,
            BinaryOperator::BitwiseShiftRight,
            Value::bitwise_shift_right,
        )
    }
}

fn binary_op<'c, T>(
    l: &Evaluated<'_>,
    r: &Evaluated<'_>,
    op: BinaryOperator,
    value_op: T,
) -> Result<Evaluated<'c>>
where
    T: FnOnce(&Value, &Value) -> Result<Value>,
{
    match (l, r) {
        (left @ (Evaluated::Number(_) | Evaluated::Text(_)), Evaluated::Value(value)) => {
            value_op(&Value::try_from(left.clone())?, value).map(Evaluated::Value)
        }
        (Evaluated::Value(value), right @ (Evaluated::Number(_) | Evaluated::Text(_))) => {
            value_op(value, &Value::try_from(right.clone())?).map(Evaluated::Value)
        }
        (Evaluated::Value(left), Evaluated::Value(right)) => {
            value_op(left, right).map(Evaluated::Value)
        }
        (left, right) => Err(EvaluateError::UnsupportedBinaryOperation {
            left: operand_error_string(left),
            op,
            right: operand_error_string(right),
        }
        .into()),
    }
}

fn unsupported_literal_binary_op(
    left: &Evaluated<'_>,
    op: BinaryOperator,
    right: &Evaluated<'_>,
) -> Error {
    EvaluateError::UnsupportedBinaryOperation {
        left: left.to_string(),
        op,
        right: right.to_string(),
    }
    .into()
}

fn incompatible_bit_operation(left: &Evaluated<'_>, right: &Evaluated<'_>) -> Error {
    EvaluateError::IncompatibleBitOperation(left.to_string(), right.to_string()).into()
}

fn literal_error_debug(value: &Evaluated<'_>) -> String {
    match value {
        Evaluated::Text(text) => format!("Literal(Text({:?}))", text.as_ref()),
        _ => value.to_string(),
    }
}

fn is_literal(value: &Evaluated<'_>) -> bool {
    matches!(value, Evaluated::Number(_) | Evaluated::Text(_))
}

fn operand_error_string(value: &Evaluated<'_>) -> String {
    if is_literal(value) {
        literal_error_debug(value)
    } else {
        format!("{value:?}")
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::str::FromStr};

    fn num(value: &str) -> Evaluated<'static> {
        Evaluated::Number(Cow::Owned(BigDecimal::from_str(value).unwrap()))
    }

    fn text(value: &str) -> Evaluated<'static> {
        Evaluated::Text(Cow::Owned(value.to_owned()))
    }

    #[test]
    fn literal_arithmetic_operations() {
        let one = num("1");
        let two = num("2");
        let zero = num("0");

        assert_eq!(one.add(&two).unwrap(), num("3"));
        assert_eq!(two.subtract(&one).unwrap(), num("1"));
        assert_eq!(one.multiply(&two).unwrap(), num("2"));
        assert_eq!(two.divide(&one).unwrap(), num("2"));
        assert_eq!(two.modulo(&one).unwrap(), num("0"));

        assert!(matches!(
            one.divide(&zero),
            Err(crate::result::Error::Evaluate(
                EvaluateError::DivisorShouldNotBeZero
            ))
        ));
        assert!(matches!(
            one.modulo(&zero),
            Err(crate::result::Error::Evaluate(
                EvaluateError::DivisorShouldNotBeZero
            ))
        ));
    }

    #[test]
    fn literal_bitwise_operations() {
        let eight = num("8");
        let two = num("2");

        assert_eq!(eight.bitwise_and(&two).unwrap(), num("0"));
        assert_eq!(eight.bitwise_shift_left(&two).unwrap(), num("32"));
        assert_eq!(eight.bitwise_shift_right(&two).unwrap(), num("2"));

        let invalid = text("foo");
        assert!(matches!(
            invalid.bitwise_and(&eight),
            Err(crate::result::Error::Evaluate(
                EvaluateError::UnsupportedBinaryOperation { .. }
            ))
        ));

        let fractional = num("2.5");
        assert!(matches!(
            eight.bitwise_shift_left(&fractional),
            Err(crate::result::Error::Evaluate(
                EvaluateError::IncompatibleBitOperation(_, _)
            ))
        ));
    }
}
