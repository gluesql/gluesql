use {
    super::Evaluated,
    crate::{
        ast::BinaryOperator,
        data::{BigDecimalExt, Value},
        executor::evaluate::{error::EvaluateError, literal::Literal},
        result::{Error, Result},
    },
    bigdecimal::BigDecimal,
    std::borrow::Cow,
};

impl<'a> Evaluated<'a> {
    pub fn add<'b, 'c>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'c>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() + r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Plus, Value::add)
    }

    pub fn subtract<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() - r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Minus, Value::subtract)
    }

    pub fn multiply<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() * r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Multiply, Value::multiply)
    }

    pub fn divide<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            if *r.as_ref() == BigDecimal::from(0) {
                return Err(EvaluateError::DivisorShouldNotBeZero.into());
            }

            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() / r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Divide, Value::divide)
    }

    pub fn modulo<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            if *r.as_ref() == BigDecimal::from(0) {
                return Err(EvaluateError::DivisorShouldNotBeZero.into());
            }

            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() % r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Modulo, Value::modulo)
    }

    pub fn bitwise_and<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(left), Evaluated::Literal(right)) = (self, other) {
            match (left, right) {
                (Literal::Number(l), Literal::Number(r)) => {
                    let lhs = l.to_i64().ok_or_else(|| {
                        unsupported_literal_binary_op(left, BinaryOperator::BitwiseAnd, right)
                    })?;
                    let rhs = r.to_i64().ok_or_else(|| {
                        unsupported_literal_binary_op(left, BinaryOperator::BitwiseAnd, right)
                    })?;

                    return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                        BigDecimal::from(lhs & rhs),
                    ))));
                }
                _ => {
                    return Err(unsupported_literal_binary_op(
                        left,
                        BinaryOperator::BitwiseAnd,
                        right,
                    ));
                }
            }
        }

        binary_op(self, other, BinaryOperator::BitwiseAnd, Value::bitwise_and)
    }

    pub fn bitwise_shift_left<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (
            Evaluated::Literal(left @ Literal::Number(l)),
            Evaluated::Literal(right @ Literal::Number(r)),
        ) = (self, other)
        {
            let lhs = l
                .to_i64()
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            if !r.is_integer_representation() {
                return Err(incompatible_bit_operation(left, right));
            }

            let rhs = r
                .to_u32()
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            let result = lhs
                .checked_shl(rhs)
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                BigDecimal::from(result),
            ))));
        }

        binary_op(
            self,
            other,
            BinaryOperator::BitwiseShiftLeft,
            Value::bitwise_shift_left,
        )
    }

    pub fn bitwise_shift_right<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (
            Evaluated::Literal(left @ Literal::Number(l)),
            Evaluated::Literal(right @ Literal::Number(r)),
        ) = (self, other)
        {
            let lhs = l
                .to_i64()
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            if !r.is_integer_representation() {
                return Err(incompatible_bit_operation(left, right));
            }

            let rhs = r
                .to_u32()
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            let result = lhs
                .checked_shr(rhs)
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                BigDecimal::from(result),
            ))));
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
        (Evaluated::Literal(literal), Evaluated::Value(value)) => {
            value_op(&Value::try_from(literal)?, value).map(Evaluated::Value)
        }
        (Evaluated::Value(value), Evaluated::Literal(literal)) => {
            value_op(value, &Value::try_from(literal)?).map(Evaluated::Value)
        }
        (Evaluated::Value(left), Evaluated::Value(right)) => {
            value_op(left, right).map(Evaluated::Value)
        }
        (left, right) => Err(EvaluateError::UnsupportedBinaryOperation {
            left: format!("{left:?}"),
            op,
            right: format!("{right:?}"),
        }
        .into()),
    }
}

fn unsupported_literal_binary_op(
    left: &Literal<'_>,
    op: BinaryOperator,
    right: &Literal<'_>,
) -> Error {
    EvaluateError::UnsupportedBinaryOperation {
        left: left.to_string(),
        op,
        right: right.to_string(),
    }
    .into()
}

fn incompatible_bit_operation(left: &Literal<'_>, right: &Literal<'_>) -> Error {
    EvaluateError::IncompatibleBitOperation(left.to_string(), right.to_string()).into()
}

#[cfg(test)]
mod tests {
    use {super::*, std::str::FromStr};

    fn num(value: &str) -> Literal<'static> {
        Literal::Number(Cow::Owned(BigDecimal::from_str(value).unwrap()))
    }

    fn text(value: &str) -> Literal<'static> {
        Literal::Text(Cow::Owned(value.to_owned()))
    }

    fn eval(literal: Literal<'static>) -> Evaluated<'static> {
        Evaluated::Literal(literal)
    }

    #[test]
    fn literal_arithmetic_operations() {
        let one = eval(num("1"));
        let two = eval(num("2"));
        let zero = eval(num("0"));

        assert_eq!(one.add(&two).unwrap(), eval(num("3")));
        assert_eq!(two.subtract(&one).unwrap(), eval(num("1")));
        assert_eq!(one.multiply(&two).unwrap(), eval(num("2")));
        assert_eq!(two.divide(&one).unwrap(), eval(num("2")));
        assert_eq!(two.modulo(&one).unwrap(), eval(num("0")));

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
        let eight = eval(num("8"));
        let two = eval(num("2"));

        assert_eq!(eight.bitwise_and(&two).unwrap(), eval(num("0")));
        assert_eq!(eight.bitwise_shift_left(&two).unwrap(), eval(num("32")));
        assert_eq!(eight.bitwise_shift_right(&two).unwrap(), eval(num("2")));

        let invalid = eval(text("foo"));
        assert!(matches!(
            invalid.bitwise_and(&eight),
            Err(crate::result::Error::Evaluate(
                EvaluateError::UnsupportedBinaryOperation { .. }
            ))
        ));

        let fractional = eval(num("2.5"));
        assert!(matches!(
            eight.bitwise_shift_left(&fractional),
            Err(crate::result::Error::Evaluate(
                EvaluateError::IncompatibleBitOperation(_, _)
            ))
        ));
    }
}
