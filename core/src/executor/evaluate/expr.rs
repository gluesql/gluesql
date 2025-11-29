use {
    super::{EvaluateError, Evaluated},
    crate::{
        ast::{AstLiteral, BinaryOperator, DataType, UnaryOperator},
        data::Value,
        executor::evaluate::evaluated::convert::text_to_value,
        result::Result,
    },
    std::{borrow::Cow, cmp::Ordering},
};

pub fn literal(ast_literal: &AstLiteral) -> Result<Evaluated<'_>> {
    match ast_literal {
        AstLiteral::Boolean(value) => Ok(Evaluated::Value(Value::Bool(*value))),
        AstLiteral::Number(value) => Ok(Evaluated::Number(Cow::Borrowed(value))),
        AstLiteral::QuotedString(value) => Ok(Evaluated::Text(Cow::Borrowed(value))),
        AstLiteral::HexString(value) => {
            let bytes = hex::decode(value)
                .map_err(|_| EvaluateError::FailedToDecodeHexString(value.clone()))?;

            Ok(Evaluated::Value(Value::Bytea(bytes)))
        }
        AstLiteral::Null => Ok(Evaluated::Value(Value::Null)),
    }
}

pub fn typed_string<'a>(data_type: &'a DataType, value: &'a str) -> Result<Evaluated<'a>> {
    text_to_value(data_type, value).map(Evaluated::Value)
}

pub fn binary_op<'a>(
    op: &BinaryOperator,
    l: Evaluated<'a>,
    r: Evaluated<'a>,
) -> Result<Evaluated<'a>> {
    macro_rules! cmp {
        ($expr: expr) => {
            Ok(Evaluated::Value(Value::Bool($expr)))
        };
    }

    macro_rules! cond {
        (l $op: tt r) => {{
            let l: bool = l.try_into()?;
            let r: bool = r.try_into()?;
            let v = l $op r;

            Ok(Evaluated::Value(Value::Bool(v)))
        }};
    }

    if l.is_null() || r.is_null() {
        return Ok(Evaluated::Value(Value::Null));
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
        return Evaluated::Value(Value::Null);
    }

    let v = low.evaluate_cmp(target) != Some(Ordering::Greater)
        && target.evaluate_cmp(high) != Some(Ordering::Greater);
    let v = negated ^ v;

    Evaluated::Value(Value::Bool(v))
}

pub fn array_index<'a>(obj: Evaluated<'a>, indexes: Vec<Evaluated<'a>>) -> Result<Evaluated<'a>> {
    let Evaluated::Value(value) = obj else {
        return Err(EvaluateError::MapOrListTypeRequired.into());
    };
    let indexes = indexes
        .into_iter()
        .map(Value::try_from)
        .collect::<Result<Vec<_>>>()?;
    value.selector_by_index(&indexes).map(Evaluated::Value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ast::AstLiteral, executor::evaluate::EvaluateError, result::Error};

    #[test]
    fn literal_converts_hex_string_to_value() {
        let ast = AstLiteral::HexString("48656c6c6f".to_owned());
        let evaluated = literal(&ast).unwrap();

        match evaluated {
            Evaluated::Value(Value::Bytea(bytes)) => assert_eq!(bytes, b"Hello".to_vec()),
            other => panic!("expected bytea value, got {other:?}"),
        }
    }

    #[test]
    fn literal_hex_string_error_propagates() {
        let ast = AstLiteral::HexString("XYZ".to_owned());
        let err = literal(&ast).unwrap_err();

        assert!(matches!(
            err,
            Error::Evaluate(EvaluateError::FailedToDecodeHexString(v)) if v == "XYZ"
        ));
    }
}
