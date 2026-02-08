use {
    super::Evaluated,
    crate::{
        ast::BinaryOperator,
        data::{BigDecimalExt, Value},
        executor::evaluate::error::EvaluateError,
        result::{Error, Result},
    },
    bigdecimal::{BigDecimal, Zero},
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
            if r.as_ref().is_zero() {
                return Err(EvaluateError::DivisorShouldNotBeZero.into());
            }

            return Ok(Evaluated::Number(Cow::Owned(l.as_ref() / r.as_ref())));
        }

        binary_op(self, other, BinaryOperator::Divide, Value::divide)
    }

    pub fn modulo<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            if r.as_ref().is_zero() {
                return Err(EvaluateError::DivisorShouldNotBeZero.into());
            }

            return Ok(Evaluated::Number(Cow::Owned(l.as_ref() % r.as_ref())));
        }

        binary_op(self, other, BinaryOperator::Modulo, Value::modulo)
    }

    pub fn bitwise_and<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            let lhs = l
                .to_i64()
                .ok_or_else(|| incompatible_bit_operation(self, other))?;
            let rhs = r
                .to_i64()
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            return Ok(Evaluated::Number(Cow::Owned(BigDecimal::from(lhs & rhs))));
        }

        binary_op(self, other, BinaryOperator::BitwiseAnd, Value::bitwise_and)
    }

    pub fn bitwise_shift_left<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Number(l), Evaluated::Number(r)) = (self, other) {
            let lhs = l
                .to_i64()
                .ok_or_else(|| incompatible_bit_operation(self, other))?;
            let rhs = r
                .to_u32()
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            let result = lhs
                .checked_shl(rhs)
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            return Ok(Evaluated::Number(Cow::Owned(BigDecimal::from(result))));
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
            let rhs = r
                .to_u32()
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            let result = lhs
                .checked_shr(rhs)
                .ok_or_else(|| incompatible_bit_operation(self, other))?;

            return Ok(Evaluated::Number(Cow::Owned(BigDecimal::from(result))));
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
            value_op(&Value::try_from(left.clone())?, value.as_ref())
                .map(|v| Evaluated::Value(Cow::Owned(v)))
        }
        (Evaluated::Value(value), right @ (Evaluated::Number(_) | Evaluated::Text(_))) => {
            value_op(value.as_ref(), &Value::try_from(right.clone())?)
                .map(|v| Evaluated::Value(Cow::Owned(v)))
        }
        (Evaluated::Value(left), Evaluated::Value(right)) => {
            value_op(left.as_ref(), right.as_ref()).map(|v| Evaluated::Value(Cow::Owned(v)))
        }
        (left, right) => Err(EvaluateError::UnsupportedBinaryOperation {
            left: left.to_string(),
            op,
            right: right.to_string(),
        }
        .into()),
    }
}

fn incompatible_bit_operation(left: &Evaluated<'_>, right: &Evaluated<'_>) -> Error {
    EvaluateError::IncompatibleBitOperation(left.to_string(), right.to_string()).into()
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

    fn val(v: Value) -> Evaluated<'static> {
        Evaluated::Value(Cow::Owned(v))
    }

    #[test]
    fn binary_op_routing() {
        assert_eq!(num("1").add(&val(Value::I64(2))), Ok(val(Value::I64(3))));
        assert_eq!(val(Value::I64(1)).add(&num("2")), Ok(val(Value::I64(3))));
        assert_eq!(
            val(Value::I64(1)).add(&val(Value::I64(2))),
            Ok(val(Value::I64(3)))
        );
        assert_eq!(
            text("a").add(&text("b")),
            Err(EvaluateError::UnsupportedBinaryOperation {
                left: "a".to_owned(),
                op: BinaryOperator::Plus,
                right: "b".to_owned(),
            }
            .into())
        );
    }

    #[test]
    fn add() {
        // fast path: Number + Number
        assert_eq!(num("1").add(&num("2")), Ok(num("3")));
        // delegation
        assert_eq!(num("1").add(&val(Value::I64(2))), Ok(val(Value::I64(3))));
    }

    #[test]
    fn subtract() {
        // fast path: Number + Number
        assert_eq!(num("3").subtract(&num("1")), Ok(num("2")));
        // delegation
        assert_eq!(
            val(Value::I64(3)).subtract(&num("1")),
            Ok(val(Value::I64(2)))
        );
    }

    #[test]
    fn multiply() {
        // fast path: Number + Number
        assert_eq!(num("2").multiply(&num("3")), Ok(num("6")));
        // delegation
        assert_eq!(
            val(Value::I64(2)).multiply(&val(Value::I64(3))),
            Ok(val(Value::I64(6)))
        );
    }

    #[test]
    fn divide() {
        // fast path: Number + Number
        assert_eq!(num("6").divide(&num("2")), Ok(num("3")));
        // delegation
        assert_eq!(num("6").divide(&val(Value::I64(2))), Ok(val(Value::I64(3))));
        // zero division error
        assert_eq!(
            num("1").divide(&num("0")),
            Err(EvaluateError::DivisorShouldNotBeZero.into())
        );
    }

    #[test]
    fn modulo() {
        // fast path: Number + Number
        assert_eq!(num("7").modulo(&num("3")), Ok(num("1")));
        // delegation
        assert_eq!(val(Value::I64(7)).modulo(&num("3")), Ok(val(Value::I64(1))));
        // zero division error
        assert_eq!(
            num("1").modulo(&num("0")),
            Err(EvaluateError::DivisorShouldNotBeZero.into())
        );
    }

    #[test]
    fn bitwise_and() {
        // fast path: Number + Number
        assert_eq!(num("29").bitwise_and(&num("15")), Ok(num("13")));
        // delegation
        assert_eq!(
            num("29").bitwise_and(&val(Value::I64(15))),
            Ok(val(Value::I64(13)))
        );
        // lhs fractional error
        assert_eq!(
            num("2.5").bitwise_and(&num("3")),
            Err(EvaluateError::IncompatibleBitOperation("2.5".to_owned(), "3".to_owned()).into())
        );
        // rhs fractional error
        assert_eq!(
            num("3").bitwise_and(&num("2.5")),
            Err(EvaluateError::IncompatibleBitOperation("3".to_owned(), "2.5".to_owned()).into())
        );
    }

    #[test]
    fn bitwise_shift_left() {
        // fast path: Number + Number
        assert_eq!(num("8").bitwise_shift_left(&num("2")), Ok(num("32")));
        // delegation
        assert_eq!(
            num("8").bitwise_shift_left(&val(Value::I64(2))),
            Ok(val(Value::I64(32)))
        );
        // lhs fractional error
        assert_eq!(
            num("2.5").bitwise_shift_left(&num("2")),
            Err(EvaluateError::IncompatibleBitOperation("2.5".to_owned(), "2".to_owned()).into())
        );
        // rhs fractional error
        assert_eq!(
            num("8").bitwise_shift_left(&num("2.5")),
            Err(EvaluateError::IncompatibleBitOperation("8".to_owned(), "2.5".to_owned()).into())
        );
        // overflow error
        assert_eq!(
            num("8").bitwise_shift_left(&num("9999999999")),
            Err(
                EvaluateError::IncompatibleBitOperation("8".to_owned(), "9999999999".to_owned())
                    .into()
            )
        );
    }

    #[test]
    fn bitwise_shift_right() {
        // fast path: Number + Number
        assert_eq!(num("32").bitwise_shift_right(&num("2")), Ok(num("8")));
        // delegation
        assert_eq!(
            num("32").bitwise_shift_right(&val(Value::I64(2))),
            Ok(val(Value::I64(8)))
        );
        // lhs fractional error
        assert_eq!(
            num("2.5").bitwise_shift_right(&num("2")),
            Err(EvaluateError::IncompatibleBitOperation("2.5".to_owned(), "2".to_owned()).into())
        );
        // rhs fractional error
        assert_eq!(
            num("32").bitwise_shift_right(&num("2.5")),
            Err(EvaluateError::IncompatibleBitOperation("32".to_owned(), "2.5".to_owned()).into())
        );
        // overflow error
        assert_eq!(
            num("32").bitwise_shift_right(&num("9999999999")),
            Err(
                EvaluateError::IncompatibleBitOperation("32".to_owned(), "9999999999".to_owned())
                    .into()
            )
        );
    }
}
