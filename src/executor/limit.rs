use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::{Expr, Offset, Value as AstValue};

use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum LimitError {
    #[error("Unreachable")]
    Unreachable,
}

pub struct Limit {
    limit: Option<usize>,
    offset: Option<usize>,
}

impl Limit {
    pub fn new(limit: Option<&Expr>, offset: Option<&Offset>) -> Result<Self> {
        let parse = |expr: &Expr| -> Result<usize> {
            match expr {
                Expr::Value(AstValue::Number(v)) => {
                    v.parse().map_err(|_| LimitError::Unreachable.into())
                }
                _ => Err(LimitError::Unreachable.into()),
            }
        };

        let limit = limit.map(|value| parse(value)).transpose()?;
        let offset = offset
            .map(|Offset { value, .. }| parse(value))
            .transpose()?;

        Ok(Self { limit, offset })
    }

    pub fn check(&self, i: usize) -> bool {
        match (self.offset, self.limit) {
            (Some(offset), Some(limit)) => i >= offset && i < offset + limit,
            (Some(offset), None) => i >= offset,
            (None, Some(limit)) => i < limit,
            (None, None) => true,
        }
    }
}
