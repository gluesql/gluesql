use {
    super::{EvaluateError, Evaluated},
    crate::{data, result::Result},
    sqlparser::ast::Value as Literal,
    std::{cmp::Ordering, convert::TryFrom},
    EvaluatedRef::*,
};

pub enum EvaluatedRef<'a> {
    Literal(&'a Literal),
    Value(&'a data::Value),
}

impl<'a> From<&'a Evaluated<'a>> for EvaluatedRef<'a> {
    fn from(evaluated: &'a Evaluated<'a>) -> Self {
        match evaluated {
            Evaluated::LiteralRef(v) => Literal(v),
            Evaluated::Literal(v) => Literal(v),
            Evaluated::ValueRef(v) => Value(v),
            Evaluated::Value(v) => Value(v),
        }
    }
}

impl<'a> PartialEq for EvaluatedRef<'a> {
    fn eq(&self, other: &EvaluatedRef<'a>) -> bool {
        match (self, other) {
            (Literal(a), Literal(b)) => a == b,
            (Literal(b), Value(a)) | (Value(a), Literal(b)) => a == b,
            (Value(a), Value(b)) => a == b,
        }
    }
}

impl<'a> PartialOrd for EvaluatedRef<'a> {
    fn partial_cmp(&self, other: &EvaluatedRef<'a>) -> Option<Ordering> {
        match (self, other) {
            (Literal(l), Literal(r)) => literal_partial_cmp(l, r),
            (Literal(l), Value(r)) => r.partial_cmp(l).map(|o| o.reverse()),
            (Value(l), Literal(r)) => l.partial_cmp(r),
            (Value(l), Value(r)) => l.partial_cmp(r),
        }
    }
}

fn literal_partial_cmp(l: &Literal, r: &Literal) -> Option<Ordering> {
    match (l, r) {
        (Literal::Number(l, false), Literal::Number(r, false)) => {
            match (l.parse::<i64>(), r.parse::<i64>()) {
                (Ok(l), Ok(r)) => Some(l.cmp(&r)),
                (_, Ok(r)) => match l.parse::<f64>() {
                    Ok(l) => l.partial_cmp(&(r as f64)),
                    _ => None,
                },
                (Ok(l), _) => match r.parse::<f64>() {
                    Ok(r) => (l as f64).partial_cmp(&r),
                    _ => None,
                },
                _ => match (l.parse::<f64>(), r.parse::<f64>()) {
                    (Ok(l), Ok(r)) => l.partial_cmp(&r),
                    _ => None,
                },
            }
        }
        (Literal::SingleQuotedString(l), Literal::SingleQuotedString(r)) => Some(l.cmp(r)),
        _ => None,
    }
}

macro_rules! binary_op {
    ($name:ident, $op:tt) => {
        pub fn $name<'b>(&self, other: &EvaluatedRef<'a>) -> Result<Evaluated<'b>> {
            let literal_binary_op = |l: &Literal, r: &Literal| match (l, r) {
                (Literal::Number(l, false), Literal::Number(r, false)) => {
                    match (l.parse::<i64>(), r.parse::<i64>()) {
                        (Ok(l), Ok(r)) => Ok(Literal::Number((l $op r).to_string(), false)),
                        (Ok(l), _) => match r.parse::<f64>() {
                            Ok(r) => Ok(Literal::Number(((l as f64) $op r).to_string(), false)),
                            _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
                        },
                        (_, Ok(r)) => match l.parse::<f64>() {
                            Ok(l) => Ok(Literal::Number((l $op (r as f64)).to_string(), false)),
                            _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
                        },
                        (_, _) => match (l.parse::<f64>(), r.parse::<f64>()) {
                            (Ok(l), Ok(r)) => Ok(Literal::Number((l $op r).to_string(), false)),
                            _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
                        },
                    }.map(Evaluated::Literal)
                }
                (Literal::Null, Literal::Number(_, false))
                | (Literal::Number(_, false), Literal::Null)
                | (Literal::Null, Literal::Null) => {
                    Ok(Evaluated::Literal(Literal::Null))
                }
                _ => Err(
                    EvaluateError::UnsupportedLiteralBinaryArithmetic(
                        l.to_string(),
                        r.to_string()
                    ).into()
                ),
            };

            let value_binary_op = |l: &data::Value, r: &data::Value| {
                l.$name(r).map(Evaluated::Value)
            };

            match (self, other) {
                (Literal(l), Literal(r)) => literal_binary_op(l, r),
                (Literal(l), Value(r)) => value_binary_op(&data::Value::try_from(*l)?, r),
                (Value(l), Literal(r)) => value_binary_op(l, &data::Value::try_from(*r)?),
                (Value(l), Value(r)) => value_binary_op(l, r),
            }
        }
    }
}

impl<'a> EvaluatedRef<'a> {
    binary_op!(add, +);
    binary_op!(subtract, -);
    binary_op!(multiply, *);
    binary_op!(divide, /);
}
