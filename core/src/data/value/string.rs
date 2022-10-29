use std::{borrow::Cow, cmp::Ordering};

use crate::data::{Literal, Value};

impl PartialEq<&str> for Value {
    fn eq(&self, other: &&str) -> bool {
        match (self, other) {
            (Value::Str(l), r) => &l[..] == *r,
            _ => false,
        }
    }
}

impl PartialOrd<&str> for Value {
    fn partial_cmp(&self, other: &&str) -> Option<Ordering> {
        match (self, other) {
            (Value::Str(l), r) => Some(l[..].cmp(r)),
            _ => None,
        }
    }
}

impl PartialEq<&str> for Literal<'_> {
    fn eq(&self, other: &&str) -> bool {
        match (self, other) {
            (&Literal::Text(Cow::Borrowed(l)), &r) => l[..] == *r,
            _ => false,
        }
    }
}

impl PartialOrd<&str> for Literal<'_> {
    fn partial_cmp(&self, other: &&str) -> Option<Ordering> {
        match (self, other) {
            (&Literal::Text(Cow::Borrowed(l)), r) => Some(l[..].cmp(r)),
            _ => None,
        }
    }
}

impl From<&str> for Value {
    fn from(str_slice: &str) -> Self {
        Value::Str(str_slice.to_owned())
    }
}
