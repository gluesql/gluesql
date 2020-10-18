use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::Ident;

use crate::data::{Row, Value};
use crate::result::Result;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum BlendContextError {
    #[error("value not found")]
    ValueNotFound,
}

#[derive(Debug)]
pub struct BlendContext<'a> {
    pub table_alias: &'a str,
    pub columns: Rc<Vec<Ident>>,
    pub row: Option<Row>,
    pub next: Option<Rc<BlendContext<'a>>>,
}

impl<'a> BlendContext<'a> {
    pub fn new(
        table_alias: &'a str,
        columns: Rc<Vec<Ident>>,
        row: Option<Row>,
        next: Option<Rc<BlendContext<'a>>>,
    ) -> Self {
        Self {
            table_alias,
            columns,
            row,
            next,
        }
    }

    pub fn get_value(&'a self, target: &str) -> Result<&'a Value> {
        let get_value = || {
            self.columns
                .iter()
                .position(|column| column.value == target)
                .and_then(|index| self.row.as_ref().and_then(|row| row.get_value(index)))
        };

        match get_value() {
            None => match &self.next {
                None => Err(BlendContextError::ValueNotFound.into()),
                Some(context) => context.get_value(target),
            },
            Some(value) => Ok(value),
        }
    }

    pub fn get_alias_value(&'a self, table_alias: &str, target: &str) -> Result<&'a Value> {
        let get_value = || {
            if self.table_alias != table_alias {
                return None;
            }

            self.columns
                .iter()
                .position(|column| column.value == target)
                .and_then(|index| self.row.as_ref().and_then(|row| row.get_value(index)))
        };

        match get_value() {
            None => match &self.next {
                None => Err(BlendContextError::ValueNotFound.into()),
                Some(context) => context.get_alias_value(table_alias, target),
            },
            Some(value) => Ok(value),
        }
    }
}
