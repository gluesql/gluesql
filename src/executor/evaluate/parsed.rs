use std::cmp::Ordering;

use sqlparser::ast::Value as AstValue;

use crate::data;
use crate::data::Value;
use crate::result::Result;

use super::EvaluateError;

pub enum Parsed<'a> {
    LiteralRef(&'a AstValue),
    Literal(AstValue),
    StringRef(&'a str),
    ValueRef(&'a Value),
    Value(Value),
}

impl<'a> PartialEq for Parsed<'a> {
    fn eq(&self, other: &Parsed<'a>) -> bool {
        let eq_ast = |l: &AstValue, r| match l {
            AstValue::SingleQuotedString(l) => l == r,
            _ => false,
        };

        let eq_val = |l: &Value, r| match l {
            Value::String(l) => l == r,
            _ => false,
        };

        {
            use Parsed::*;

            match self {
                LiteralRef(l) => match other {
                    LiteralRef(r) => l == r,
                    StringRef(r) => eq_ast(l, r),
                    ValueRef(r) => r == l,
                    Value(r) => &r == l,
                    Literal(_) => panic!(),
                },
                StringRef(l) => match other {
                    LiteralRef(r) => eq_ast(r, l),
                    StringRef(r) => l == r,
                    ValueRef(r) => eq_val(r, l),
                    Value(r) => eq_val(&r, l),
                    Literal(_) => false,
                },
                ValueRef(l) => match other {
                    LiteralRef(r) => l == r,
                    Literal(r) => l == &r,
                    StringRef(r) => eq_val(l, r),
                    ValueRef(r) => l == r,
                    Value(r) => l == &r,
                },
                Value(l) => match other {
                    LiteralRef(r) => &l == r,
                    StringRef(r) => eq_val(&l, r),
                    ValueRef(r) => &l == r,
                    Value(r) => l == r,
                    Literal(_) => panic!(),
                },
                Literal(l) => match other {
                    ValueRef(r) => r == &l,
                    StringRef(_) => false,
                    _ => panic!(),
                },
            }
        }
    }
}

impl<'a> PartialOrd for Parsed<'a> {
    fn partial_cmp(&self, other: &Parsed<'a>) -> Option<Ordering> {
        use Parsed::*;

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_partial_cmp(l, r),
                ValueRef(r) => r.partial_cmp(l).map(|o| o.reverse()),
                Value(r) => r.partial_cmp(*l).map(|o| o.reverse()),
                StringRef(_) => None,
                Literal(_) => panic!(),
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.partial_cmp(r),
                ValueRef(r) => l.partial_cmp(r),
                Value(r) => l.partial_cmp(&r),
                StringRef(r) => match l {
                    data::Value::String(l) => (&l.as_str()).partial_cmp(r),
                    _ => None,
                },
                Literal(_) => panic!(),
            },
            Value(l) => match other {
                LiteralRef(r) => l.partial_cmp(*r),
                ValueRef(r) => l.partial_cmp(*r),
                Value(r) => l.partial_cmp(r),
                StringRef(r) => match l {
                    data::Value::String(l) => (&l.as_str()).partial_cmp(r),
                    _ => None,
                },
                Literal(_) => panic!(),
            },
            StringRef(l) => match other {
                LiteralRef(_) => None,
                ValueRef(data::Value::String(r)) => l.partial_cmp(&r.as_str()),
                Value(data::Value::String(r)) => l.partial_cmp(&r.as_str()),
                StringRef(r) => l.partial_cmp(r),
                Literal(_) => panic!(),
                _ => None,
            },
            Literal(_) => panic!(),
        }
    }
}

fn literal_partial_cmp(a: &AstValue, b: &AstValue) -> Option<Ordering> {
    match (a, b) {
        (AstValue::Number(l), AstValue::Number(r)) => match (l.parse::<i64>(), r.parse::<i64>()) {
            (Ok(l), Ok(r)) => Some(l.cmp(&r)),
            _ => None,
        },
        (AstValue::SingleQuotedString(l), AstValue::SingleQuotedString(r)) => Some(l.cmp(r)),
        _ => None,
    }
}

impl<'a> Parsed<'a> {
    pub fn add(&self, other: &Parsed<'a>) -> Result<Parsed<'a>> {
        use Parsed::*;

        let unreachable = Err(EvaluateError::UnreachableParsedArithmetic.into());

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_add(l, r).map(Parsed::Literal),
                ValueRef(r) => r.add(&r.clone_by(l)?).map(Parsed::Value),
                Value(_) | StringRef(_) | Literal(_) => unreachable,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.add(&l.clone_by(r)?).map(Parsed::Value),
                ValueRef(r) => l.add(r).map(Parsed::Value),
                Value(_) | StringRef(_) | Literal(_) => unreachable,
            },
            Value(_) | StringRef(_) | Literal(_) => unreachable,
        }
    }

    pub fn subtract(&self, other: &Parsed<'a>) -> Result<Parsed<'a>> {
        use Parsed::*;

        let unreachable = Err(EvaluateError::UnreachableParsedArithmetic.into());

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_subtract(l, r).map(Parsed::Literal),
                ValueRef(r) => r.rsubtract(&r.clone_by(l)?).map(Parsed::Value),
                Value(_) | StringRef(_) | Literal(_) => unreachable,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.subtract(&l.clone_by(r)?).map(Parsed::Value),
                ValueRef(r) => l.subtract(r).map(Parsed::Value),
                Value(_) | StringRef(_) | Literal(_) => unreachable,
            },
            Value(_) | StringRef(_) | Literal(_) => unreachable,
        }
    }

    pub fn multiply(&self, other: &Parsed<'a>) -> Result<Parsed<'a>> {
        use Parsed::*;

        let unreachable = Err(EvaluateError::UnreachableParsedArithmetic.into());

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_multiply(l, r).map(Parsed::Literal),
                ValueRef(r) => r.multiply(&r.clone_by(l)?).map(Parsed::Value),
                Value(_) | StringRef(_) | Literal(_) => unreachable,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.multiply(&l.clone_by(r)?).map(Parsed::Value),
                ValueRef(r) => l.multiply(r).map(Parsed::Value),
                Value(_) | StringRef(_) | Literal(_) => unreachable,
            },
            Value(_) | StringRef(_) | Literal(_) => unreachable,
        }
    }

    pub fn divide(&self, other: &Parsed<'a>) -> Result<Parsed<'a>> {
        use Parsed::*;

        let unreachable = Err(EvaluateError::UnreachableParsedArithmetic.into());

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_divide(l, r).map(Parsed::Literal),
                ValueRef(r) => r.rdivide(&r.clone_by(l)?).map(Parsed::Value),
                Value(_) | StringRef(_) | Literal(_) => unreachable,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.divide(&l.clone_by(r)?).map(Parsed::Value),
                ValueRef(r) => l.divide(r).map(Parsed::Value),
                Value(_) | StringRef(_) | Literal(_) => unreachable,
            },
            Value(_) | StringRef(_) | Literal(_) => unreachable,
        }
    }
}

fn literal_add(a: &AstValue, b: &AstValue) -> Result<AstValue> {
    match (a, b) {
        (AstValue::Number(a), AstValue::Number(b)) => match (a.parse::<i64>(), b.parse::<i64>()) {
            (Ok(a), Ok(b)) => Ok(AstValue::Number((a + b).to_string())),
            _ => panic!(),
        },
        _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
    }
}

fn literal_subtract(a: &AstValue, b: &AstValue) -> Result<AstValue> {
    match (a, b) {
        (AstValue::Number(a), AstValue::Number(b)) => match (a.parse::<i64>(), b.parse::<i64>()) {
            (Ok(a), Ok(b)) => Ok(AstValue::Number((a - b).to_string())),
            _ => panic!(),
        },
        _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
    }
}

fn literal_multiply(a: &AstValue, b: &AstValue) -> Result<AstValue> {
    match (a, b) {
        (AstValue::Number(a), AstValue::Number(b)) => match (a.parse::<i64>(), b.parse::<i64>()) {
            (Ok(a), Ok(b)) => Ok(AstValue::Number((a * b).to_string())),
            _ => panic!(),
        },
        _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
    }
}

fn literal_divide(a: &AstValue, b: &AstValue) -> Result<AstValue> {
    match (a, b) {
        (AstValue::Number(a), AstValue::Number(b)) => match (a.parse::<i64>(), b.parse::<i64>()) {
            (Ok(a), Ok(b)) => Ok(AstValue::Number((a - b).to_string())),
            _ => panic!(),
        },
        _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
    }
}
