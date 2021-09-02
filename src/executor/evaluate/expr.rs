use itertools::Itertools;

use crate::EvaluateError;

use {
    super::Evaluated,
    crate::{
        ast::{AstLiteral, BinaryOperator, DataType, UnaryOperator},
        data::{Literal, Value},
        result::Result,
    },
    std::{
        borrow::Cow,
        convert::{TryFrom, TryInto},
    },
};

pub fn literal(ast_literal: &AstLiteral) -> Result<Evaluated<'_>> {
    Literal::try_from(ast_literal).map(Evaluated::Literal)
}

pub fn typed_string<'a>(data_type: &'a DataType, value: Cow<'a, String>) -> Result<Evaluated<'a>> {
    let literal = Literal::Text(value);

    Value::try_from_literal(data_type, &literal).map(Evaluated::from)
}

pub fn binary_op<'a>(
    op: &BinaryOperator,
    l: Evaluated<'a>,
    r: Evaluated<'a>,
) -> Result<Evaluated<'a>> {
    macro_rules! cmp {
        ($expr: expr) => {
            Ok(Evaluated::from(Value::Bool($expr)))
        };
    }

    macro_rules! cond {
        (l $op: tt r) => {{
            let l: bool = l.try_into()?;
            let r: bool = r.try_into()?;
            let v = l $op r;

            Ok(Evaluated::from(Value::Bool(v)))
        }};
    }

    match op {
        BinaryOperator::Plus => l.add(&r),
        BinaryOperator::Minus => l.subtract(&r),
        BinaryOperator::Multiply => l.multiply(&r),
        BinaryOperator::Divide => l.divide(&r),
        BinaryOperator::Modulo => l.modulo(&r),
        BinaryOperator::StringConcat => l.concat(r),
        BinaryOperator::Eq => cmp!(l == r),
        BinaryOperator::NotEq => cmp!(l != r),
        BinaryOperator::Lt => cmp!(l < r),
        BinaryOperator::LtEq => cmp!(l <= r),
        BinaryOperator::Gt => cmp!(l > r),
        BinaryOperator::GtEq => cmp!(l >= r),
        BinaryOperator::And => cond!(l && r),
        BinaryOperator::Or => cond!(l || r),
        BinaryOperator::Like => l.like(r, true),
        BinaryOperator::ILike => l.like(r, false),
        BinaryOperator::NotLike => {
            cmp!(l.like(r, true)? == Evaluated::Literal(Literal::Boolean(false)))
        }
        BinaryOperator::NotILike => {
            cmp!(l.like(r, false)? == Evaluated::Literal(Literal::Boolean(false)))
        }
    }
}

pub fn unary_op<'a>(op: &UnaryOperator, v: Evaluated<'a>) -> Result<Evaluated<'a>> {
    match op {
        UnaryOperator::Plus => v.unary_plus(),
        UnaryOperator::Minus => v.unary_minus(),
        UnaryOperator::Not => v.try_into().map(|v: bool| Evaluated::from(Value::Bool(!v))),
    }
}

pub fn between<'a>(
    target: Evaluated<'a>,
    negated: bool,
    low: Evaluated<'a>,
    high: Evaluated<'a>,
) -> Result<Evaluated<'a>> {
    let v = low <= target && target <= high;
    let v = negated ^ v;

    Ok(Evaluated::from(Value::Bool(v)))
}

pub fn case<'a>(
    operand: Option<Evaluated<'a>>,
    when_then: Vec<(Evaluated<'a>, Evaluated<'a>)>,
    else_result: Option<Evaluated<'a>>,
) -> Result<Evaluated<'a>> {
    use std::mem::discriminant as disc;

    let (_, then_results) = when_then.iter().cloned().unzip::<_, _, Vec<_>, Vec<_>>();

    let results = match &else_result {
        Some(result) => [&then_results[..], &[result.to_owned()]].concat(),
        None => then_results,
    };

    let type_diff = results
        .iter()
        .map(|r| -> Value { r.to_owned().try_into().unwrap() })
        .tuple_windows()
        .any(|(left, right)| disc(&left) != disc(&right));

    if type_diff {
        Err(EvaluateError::UnequalResultTypes("CASE".to_owned()).into())
    } else {
        match operand {
            Some(o) => match when_then.iter().find(|(when, _)| when.eq(&o)) {
                Some((_, then)) => Ok(then.to_owned()),
                None => match else_result {
                    Some(result) => Ok(result),
                    None => Ok(Evaluated::from(Value::Null)),
                },
            },
            None => {
                let thens = when_then
                    .iter()
                    .map(|(when, then)| match when.to_owned().try_into() {
                        Ok(Value::Bool(condition)) => {
                            if condition {
                                Ok(Some(then))
                            } else {
                                Ok(None)
                            }
                        }
                        _ => Err(()),
                    })
                    .collect::<Vec<_>>();

                if thens.iter().any(|&wt| wt.is_err()) {
                    Err(EvaluateError::BooleanTypeRequired("CASE".to_owned()).into())
                } else {
                    match thens
                        .iter()
                        .map(|&x| x.unwrap())
                        .find(|&then| then.is_some())
                    {
                        Some(then) => Ok(then.unwrap().to_owned()),
                        None => match else_result {
                            Some(result) => Ok(result),
                            None => Ok(Evaluated::from(Value::Null)),
                        },
                    }
                }
            }
        }
    }
}
