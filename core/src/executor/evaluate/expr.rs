use {
    super::{EvaluateError, Evaluated},
    crate::{
        ast::{AstLiteral, BinaryOperator, DataType, UnaryOperator},
        data::{Literal, Value},
        result::Result,
    },
    std::{borrow::Cow, cmp::Ordering},
};

pub fn literal(ast_literal: &AstLiteral) -> Result<Evaluated<'_>> {
    Literal::try_from(ast_literal).map(Evaluated::Literal)
}

pub fn typed_string<'a>(data_type: &'a DataType, value: Cow<'a, str>) -> Result<Evaluated<'a>> {
    let literal = Literal::Text(value);

    Value::try_from_literal(data_type, &literal).map(Evaluated::Value)
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

    match op {
        BinaryOperator::Plus => l.add(&r),
        BinaryOperator::Minus => l.subtract(&r),
        BinaryOperator::Multiply => l.multiply(&r),
        BinaryOperator::Divide => l.divide(&r),
        BinaryOperator::Modulo => l.modulo(&r),
        BinaryOperator::StringConcat => l.concat(r),
        BinaryOperator::Eq => cmp!(l.evaluate_eq(&r)),
        BinaryOperator::NotEq => cmp!(!l.evaluate_eq(&r)),
        BinaryOperator::Lt => cmp!(l.evaluate_cmp(&r) == Some(Ordering::Less)),
        BinaryOperator::LtEq => cmp!(matches!(
            l.evaluate_cmp(&r),
            Some(Ordering::Less) | Some(Ordering::Equal)
        )),
        BinaryOperator::Gt => cmp!(l.evaluate_cmp(&r) == Some(Ordering::Greater)),
        BinaryOperator::GtEq => cmp!(matches!(
            l.evaluate_cmp(&r),
            Some(Ordering::Greater) | Some(Ordering::Equal)
        )),
        BinaryOperator::And => cond!(l && r),
        BinaryOperator::Or => cond!(l || r),
        BinaryOperator::Xor => cond!(l ^ r),
        BinaryOperator::BitwiseAnd => l.bitwise_and(&r),
        BinaryOperator::BitwiseShiftLeft => l.bitwise_shift_left(&r),
        BinaryOperator::BitwiseShiftRight => l.bitwise_shift_right(&r),
    }
}

pub fn unary_op<'a>(op: &UnaryOperator, v: Evaluated<'a>) -> Result<Evaluated<'a>> {
    match op {
        UnaryOperator::Plus => v.unary_plus(),
        UnaryOperator::Minus => v.unary_minus(),
        UnaryOperator::Not => v
            .try_into()
            .map(|v: bool| Evaluated::Value(Value::Bool(!v))),
        UnaryOperator::Factorial => v.unary_factorial(),
        UnaryOperator::BitwiseNot => v.unary_bitwise_not(),
    }
}

pub fn between<'a>(
    target: Evaluated<'a>,
    negated: bool,
    low: Evaluated<'a>,
    high: Evaluated<'a>,
) -> Result<Evaluated<'a>> {
    let v = low.evaluate_cmp(&target) != Some(Ordering::Greater)
        && target.evaluate_cmp(&high) != Some(Ordering::Greater);
    let v = negated ^ v;

    Ok(Evaluated::Value(Value::Bool(v)))
}

pub fn array_index<'a>(obj: Evaluated<'a>, indexes: Vec<Evaluated<'a>>) -> Result<Evaluated<'a>> {
    let value = match obj {
        Evaluated::Value(value) => value,
        _ => return Err(EvaluateError::MapOrListTypeRequired.into()),
    };
    let indexes = indexes
        .into_iter()
        .map(Value::try_from)
        .collect::<Result<Vec<_>>>()?;
    value.selector_by_index(&indexes).map(Evaluated::Value)
}
