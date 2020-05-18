use boolinator::Boolinator;
use nom_sql::{Literal, SelectStatement};
use std::cmp::Ordering;
use std::fmt::Debug;

use crate::data::Value;
use crate::executor::{fetch_select_params, select, FilterContext};
use crate::result::Result;
use crate::storage::Store;

use super::FilterError;

pub enum Parsed<'a> {
    LiteralRef(&'a Literal),
    Literal(Literal),
    ValueRef(&'a Value),
    Value(Value),
}

pub enum ParsedList<'a, T: 'static + Debug> {
    LiteralRef(&'a Vec<Literal>),
    Value {
        storage: &'a dyn Store<T>,
        statement: &'a SelectStatement,
        filter_context: &'a FilterContext<'a>,
    },
    Parsed(Parsed<'a>),
}

impl<'a> PartialEq for Parsed<'a> {
    fn eq(&self, other: &Parsed<'a>) -> bool {
        use Parsed::*;

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => l == r,
                Literal(r) => l == &r,
                ValueRef(r) => r == l,
                Value(r) => &r == l,
            },
            Literal(l) => match other {
                LiteralRef(r) => &l == r,
                Literal(r) => l == r,
                ValueRef(r) => r == &l,
                Value(r) => &r == &l,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l == r,
                Literal(r) => l == &r,
                ValueRef(r) => l == r,
                Value(r) => l == &r,
            },
            Value(l) => match other {
                LiteralRef(r) => &l == r,
                Literal(r) => l == r,
                ValueRef(r) => &l == r,
                Value(r) => l == r,
            },
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
                Literal(_) => None,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.partial_cmp(r),
                ValueRef(r) => l.partial_cmp(r),
                Value(r) => l.partial_cmp(&r),
                Literal(_) => None,
            },
            Value(l) => match other {
                LiteralRef(r) => l.partial_cmp(*r),
                ValueRef(r) => l.partial_cmp(*r),
                Value(r) => l.partial_cmp(r),
                Literal(_) => None,
            },
            Literal(_) => None,
        }
    }
}

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
                let params = fetch_select_params(storage, statement)?;
                let v = select(storage, statement, &params, Some(filter_context))?
                    .map(|row| row?.take_first_value())
                    .filter_map(|value| {
                        value.map_or_else(
                            |error| Some(Err(error)),
                            |value| (&Parsed::Value(value) == self).as_some(Ok(())),
                        )
                    })
                    .next()
                    .transpose()?
                    .is_some();

                v
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

fn literal_partial_cmp(a: &Literal, b: &Literal) -> Option<Ordering> {
    match (a, b) {
        (Literal::String(a), Literal::String(b)) => Some(a.cmp(b)),
        (Literal::Integer(a), Literal::Integer(b)) => Some(a.cmp(b)),
        _ => None,
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
