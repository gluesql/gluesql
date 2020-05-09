use boolinator::Boolinator;
use nom_sql::{Literal, SelectStatement};
use std::cmp::Ordering;
use std::fmt::Debug;

use crate::data::{literal_partial_cmp, Value};
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

        match (self, other) {
            (LiteralRef(lr), ValueRef(vr)) => vr.partial_cmp(lr).map(|o| o.reverse()),
            (ValueRef(vr), LiteralRef(lr)) => vr.partial_cmp(lr),
            (LiteralRef(lr), Value(v)) => v.partial_cmp(*lr).map(|o| o.reverse()),
            (Value(v), LiteralRef(lr)) => v.partial_cmp(*lr),
            (Value(v), ValueRef(vr)) => v.partial_cmp(*vr),
            (ValueRef(vr), Value(v)) => v.partial_cmp(*vr).map(|o| o.reverse()),
            (Value(v), Value(v2)) => v.partial_cmp(v2),
            (ValueRef(vr), ValueRef(vr2)) => vr.partial_cmp(vr2),
            (LiteralRef(lr), LiteralRef(lr2)) => literal_partial_cmp(lr, lr2),
            (Literal(_), _) | (_, Literal(_)) => None,
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
                LiteralRef(r) => literal_add(l, r).map(|v| Parsed::Literal(v)),
                ValueRef(r) => r.add(&r.clone_by(l)?).map(|v| Parsed::Value(v)),
                Value(_) | Literal(_) => unreachable,
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.add(&l.clone_by(r)?).map(|v| Parsed::Value(v)),
                ValueRef(r) => l.add(r).map(|v| Parsed::Value(v)),
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
