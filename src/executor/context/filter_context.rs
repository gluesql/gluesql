use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::Ident;

use crate::data::{Row, Value};
use crate::result::Result;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum FilterContextError {
    #[error("value not found")]
    ValueNotFound,
}

#[derive(Debug)]
pub struct FilterContext<'a> {
    table_alias: &'a str,
    columns: &'a [Ident],
    row: Option<&'a Row>,
    next: Option<Rc<FilterContext<'a>>>,
}

impl<'a> FilterContext<'a> {
    pub fn new(
        table_alias: &'a str,
        columns: &'a [Ident],
        row: Option<&'a Row>,
        next: Option<Rc<FilterContext<'a>>>,
    ) -> Self {
        Self {
            table_alias,
            columns,
            row,
            next,
        }
    }

    pub fn get_value(&self, target: &str) -> Result<Option<&'a Value>> {
        let get_value = || {
            self.columns
                .iter()
                .position(|column| column.value == target)
                .map(|index| self.row.and_then(|row| row.get_value(index)))
        };

        match get_value() {
            None => match &self.next {
                None => Err(FilterContextError::ValueNotFound.into()),
                Some(context) => context.get_value(target),
            },
            Some(value) => Ok(value),
        }
    }

    pub fn get_alias_value(&self, table_alias: &str, target: &str) -> Result<Option<&'a Value>> {
        let get_value = || {
            if self.table_alias != table_alias {
                return None;
            }

            self.columns
                .iter()
                .position(|column| column.value == target)
                .map(|index| self.row.and_then(|row| row.get_value(index)))
        };

        match get_value() {
            None => match &self.next {
                None => Err(FilterContextError::ValueNotFound.into()),
                Some(context) => context.get_alias_value(table_alias, target),
            },
            Some(value) => Ok(value),
        }
    }
}
