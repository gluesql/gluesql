use {
    super::{EvaluateError, Evaluated},
    crate::{
        ast::{BinaryOperator, DataType, Literal, UnaryOperator},
        data::Value,
        executor::evaluate::evaluated::convert::text_to_value,
        result::Result,
    },
    std::{borrow::Cow, cmp::Ordering},
};

pub fn literal(literal: &Literal) -> Evaluated<'_> {
    match literal {
        Literal::Number(value) => Evaluated::Number(Cow::Borrowed(value)),
        Literal::QuotedString(value) => Evaluated::Text(Cow::Borrowed(value)),
    }
}

pub fn typed_string<'a>(data_type: &'a DataType, value: &'a str) -> Result<Evaluated<'a>> {
    text_to_value(data_type, value).map(|v| Evaluated::Value(Cow::Owned(v)))
}

pub fn binary_op<'a>(
    op: &BinaryOperator,
    l: Evaluated<'a>,
    r: Evaluated<'a>,
) -> Result<Evaluated<'a>> {
    macro_rules! cmp {
        ($expr: expr) => {
            Ok(Evaluated::Value(Cow::Owned(Value::Bool($expr))))
        };
    }

    macro_rules! cond {
        (l $op: tt r) => {{
            let l: bool = l.try_into()?;
            let r: bool = r.try_into()?;
            let v = l $op r;

            Ok(Evaluated::Value(Cow::Owned(Value::Bool(v))))
        }};
    }

    if l.is_null() || r.is_null() {
        return Ok(Evaluated::Value(Cow::Owned(Value::Null)));
    }

    match op {
        BinaryOperator::Plus => l.add(&r),
        BinaryOperator::Minus => l.subtract(&r),
        BinaryOperator::Multiply => l.multiply(&r),
        BinaryOperator::Divide => l.divide(&r),
        BinaryOperator::Modulo => l.modulo(&r),
        BinaryOperator::StringConcat => Ok(l.concat(r)),
        BinaryOperator::Eq => Ok(Evaluated::from(l.evaluate_eq(&r))),
        BinaryOperator::NotEq => Ok(Evaluated::from(!l.evaluate_eq(&r))),
        BinaryOperator::Lt => cmp!(l.evaluate_cmp(&r) == Some(Ordering::Less)),
        BinaryOperator::LtEq => cmp!(matches!(
            l.evaluate_cmp(&r),
            Some(Ordering::Less | Ordering::Equal)
        )),
        BinaryOperator::Gt => cmp!(l.evaluate_cmp(&r) == Some(Ordering::Greater)),
        BinaryOperator::GtEq => cmp!(matches!(
            l.evaluate_cmp(&r),
            Some(Ordering::Greater | Ordering::Equal)
        )),
        BinaryOperator::And => cond!(l && r),
        BinaryOperator::Or => cond!(l || r),
        BinaryOperator::Xor => cond!(l ^ r),
        BinaryOperator::BitwiseAnd => l.bitwise_and(&r),
        BinaryOperator::BitwiseShiftLeft => l.bitwise_shift_left(&r),
        BinaryOperator::BitwiseShiftRight => l.bitwise_shift_right(&r),
        BinaryOperator::Arrow => l.arrow(&r),
        BinaryOperator::LongArrow => l.long_arrow(&r),
    }
}

pub fn unary_op<'a>(op: &UnaryOperator, v: Evaluated<'a>) -> Result<Evaluated<'a>> {
    match op {
        UnaryOperator::Plus => v.unary_plus(),
        UnaryOperator::Minus => v.unary_minus(),
        UnaryOperator::Not => v.unary_not(),
        UnaryOperator::Factorial => v.unary_factorial(),
        UnaryOperator::BitwiseNot => v.unary_bitwise_not(),
    }
}

pub fn between<'a>(
    target: &Evaluated<'a>,
    negated: bool,
    low: &Evaluated<'a>,
    high: &Evaluated<'a>,
) -> Evaluated<'a> {
    if target.is_null() || low.is_null() || high.is_null() {
        return Evaluated::Value(Cow::Owned(Value::Null));
    }

    let v = low.evaluate_cmp(target) != Some(Ordering::Greater)
        && target.evaluate_cmp(high) != Some(Ordering::Greater);
    let v = negated ^ v;

    Evaluated::Value(Cow::Owned(Value::Bool(v)))
}

pub fn array_index<'a>(obj: Evaluated<'a>, indexes: Vec<Evaluated<'a>>) -> Result<Evaluated<'a>> {
    let Evaluated::Value(value) = obj else {
        return Err(EvaluateError::MapOrListTypeRequired.into());
    };
    let indexes = indexes
        .into_iter()
        .map(Value::try_from)
        .collect::<Result<Vec<_>>>()?;
    value
        .into_owned()
        .selector_by_index(&indexes)
        .map(|v| Evaluated::Value(Cow::Owned(v)))
}

#[cfg(test)]
mod tests {
    use {
        super::{Evaluated, literal},
        crate::ast::Literal,
        bigdecimal::BigDecimal,
        std::borrow::Cow,
    };

    #[test]
    fn test_literal() {
        assert_eq!(
            literal(&Literal::Number(BigDecimal::from(42))),
            Evaluated::Number(Cow::Owned(BigDecimal::from(42)))
        );
        assert_eq!(
            literal(&Literal::QuotedString("hello".to_owned())),
            Evaluated::Text(Cow::Owned("hello".to_owned()))
        );
    }
}
