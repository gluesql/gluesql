// use boolinator::Boolinator;
use std::cmp::Ordering;
// use std::fmt::Debug;

use sqlparser::ast::Value as AstValue;

use crate::data;
use crate::data::Value;
// use crate::executor::{select, FilterContext};
// use crate::result::Result;
// use crate::storage::Store;

// use super::FilterError;

pub enum Parsed<'a> {
    LiteralRef(&'a AstValue),
    StringRef(&'a str),
    // Literal(Literal),
    ValueRef(&'a Value),
    Value(Value),
}

/*
pub enum ParsedList<'a, T: 'static + Debug> {
    LiteralRef(&'a Vec<AstValue>),
    Value {
        storage: &'a dyn Store<T>,
        query: &'a Query,
        filter_context: &'a FilterContext<'a>,
    },
    Parsed(Parsed<'a>),
}
*/

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
                },
                StringRef(l) => match other {
                    LiteralRef(r) => eq_ast(r, l),
                    StringRef(r) => l == r,
                    ValueRef(r) => eq_val(r, l),
                    Value(r) => eq_val(&r, l),
                },
                ValueRef(l) => match other {
                    LiteralRef(r) => l == r,
                    StringRef(r) => eq_val(l, r),
                    ValueRef(r) => l == r,
                    Value(r) => l == &r,
                },
                Value(l) => match other {
                    LiteralRef(r) => &l == r,
                    StringRef(r) => eq_val(&l, r),
                    ValueRef(r) => &l == r,
                    Value(r) => l == r,
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
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.partial_cmp(r),
                ValueRef(r) => l.partial_cmp(r),
                Value(r) => l.partial_cmp(&r),
                StringRef(r) => match l {
                    data::Value::String(l) => (&l.as_str()).partial_cmp(r),
                    _ => None,
                },
            },
            Value(l) => match other {
                LiteralRef(r) => l.partial_cmp(*r),
                ValueRef(r) => l.partial_cmp(*r),
                Value(r) => l.partial_cmp(r),
                StringRef(r) => match l {
                    data::Value::String(l) => (&l.as_str()).partial_cmp(r),
                    _ => None,
                },
            },
            StringRef(l) => match other {
                LiteralRef(_) => None,
                ValueRef(data::Value::String(r)) => l.partial_cmp(&r.as_str()),
                Value(data::Value::String(r)) => l.partial_cmp(&r.as_str()),
                StringRef(r) => l.partial_cmp(r),
                _ => None,
            },
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

/*
impl<'a> Parsed<'a> {
    pub fn exists_in<T: 'static + Debug>(&self, list: ParsedList<'_, T>) -> Result<bool> {
        Ok(match list {
            ParsedList::Parsed(parsed) => &parsed == self,
            ParsedList::LiteralRef(literals) => literals
                .iter()
                .any(|literal| &Parsed::LiteralRef(&literal) == self),
            ParsedList::Value {
                storage,
                statement,
                filter_context,
            } => {
                let value = select(storage, statement, Some(filter_context))?
                    .map(|row| row?.take_first_value())
                    .filter_map(|value| {
                        value.map_or_else(
                            |error| Some(Err(error)),
                            |value| (&Parsed::Value(value) == self).as_some(Ok(())),
                        )
                    })
                    .next();

                value.transpose()?.is_some()
            }
        })
    }

    pub fn add(&self, other: &Parsed<'a>) -> Result<Parsed<'a>> {
        use Parsed::*;

        let unreachable = Err(FilterError::UnreachableParsedArithmetic.into());

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_add(l, r).map(Parsed::Literal),
                ValueRef(r) => r.add(&r.clone_by(l)?).map(Parsed::Value),
                Value(_) | Literal(_) => unreachable,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.add(&l.clone_by(r)?).map(Parsed::Value),
                ValueRef(r) => l.add(r).map(Parsed::Value),
                Value(_) | Literal(_) => unreachable,
            },
            Literal(_) | Value(_) => unreachable,
        }
    }

    pub fn subtract(&self, other: &Parsed<'a>) -> Result<Parsed<'a>> {
        use Parsed::*;

        let unreachable = Err(FilterError::UnreachableParsedArithmetic.into());

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_subtract(l, r).map(Parsed::Literal),
                ValueRef(r) => r.rsubtract(&r.clone_by(l)?).map(Parsed::Value),
                Value(_) | Literal(_) => unreachable,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.subtract(&l.clone_by(r)?).map(Parsed::Value),
                ValueRef(r) => l.subtract(r).map(Parsed::Value),
                Value(_) | Literal(_) => unreachable,
            },
            Literal(_) | Value(_) => unreachable,
        }
    }

    pub fn multiply(&self, other: &Parsed<'a>) -> Result<Parsed<'a>> {
        use Parsed::*;

        let unreachable = Err(FilterError::UnreachableParsedArithmetic.into());

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_multiply(l, r).map(Parsed::Literal),
                ValueRef(r) => r.multiply(&r.clone_by(l)?).map(Parsed::Value),
                Value(_) | Literal(_) => unreachable,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.multiply(&l.clone_by(r)?).map(Parsed::Value),
                ValueRef(r) => l.multiply(r).map(Parsed::Value),
                Value(_) | Literal(_) => unreachable,
            },
            Literal(_) | Value(_) => unreachable,
        }
    }

    pub fn divide(&self, other: &Parsed<'a>) -> Result<Parsed<'a>> {
        use Parsed::*;

        let unreachable = Err(FilterError::UnreachableParsedArithmetic.into());

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_divide(l, r).map(Parsed::Literal),
                ValueRef(r) => r.rdivide(&r.clone_by(l)?).map(Parsed::Value),
                Value(_) | Literal(_) => unreachable,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.divide(&l.clone_by(r)?).map(Parsed::Value),
                ValueRef(r) => l.divide(r).map(Parsed::Value),
                Value(_) | Literal(_) => unreachable,
            },
            Literal(_) | Value(_) => unreachable,
        }
    }
}

fn literal_add(a: &Literal, b: &Literal) -> Result<Literal> {
    match (a, b) {
        (Literal::Integer(a), Literal::Integer(b)) => Ok(Literal::Integer(a + b)),
        _ => Err(FilterError::UnreachableLiteralArithmetic.into()),
    }
}

fn literal_subtract(a: &Literal, b: &Literal) -> Result<Literal> {
    match (a, b) {
        (Literal::Integer(a), Literal::Integer(b)) => Ok(Literal::Integer(a - b)),
        _ => Err(FilterError::UnreachableLiteralArithmetic.into()),
    }
}

fn literal_multiply(a: &Literal, b: &Literal) -> Result<Literal> {
    match (a, b) {
        (Literal::Integer(a), Literal::Integer(b)) => Ok(Literal::Integer(a * b)),
        _ => Err(FilterError::UnreachableLiteralArithmetic.into()),
    }
}

fn literal_divide(a: &Literal, b: &Literal) -> Result<Literal> {
    match (a, b) {
        (Literal::Integer(a), Literal::Integer(b)) => Ok(Literal::Integer(a / b)),
        _ => Err(FilterError::UnreachableLiteralArithmetic.into()),
    }
}
*/
