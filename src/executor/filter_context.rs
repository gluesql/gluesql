use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::Ident;

use crate::data::{Row, Value};
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum FilterContextError {
    #[error("value not found")]
    ValueNotFound,
}

#[derive(Debug)]
pub struct FilterContext<'a> {
    table_alias: &'a str,
    columns: &'a [Ident],
    row: &'a Row,
    next: Option<&'a FilterContext<'a>>,
}

impl<'a> FilterContext<'a> {
    pub fn new(
        table_alias: &'a str,
        columns: &'a [Ident],
        row: &'a Row,
        next: Option<&'a FilterContext<'a>>,
    ) -> Self {
        Self {
            table_alias,
            columns,
            row,
            next,
        }
    }

    pub fn get_value(&self, target: &str) -> Result<&'a Value> {
        let get_value = || {
            self.columns
                .iter()
                .position(|column| column.value == target)
                .and_then(|index| self.row.get_value(index))
        };

        match get_value() {
            None => match self.next {
                None => Err(FilterContextError::ValueNotFound.into()),
                Some(context) => context.get_value(target),
            },
            Some(value) => Ok(value),
        }
    }

    pub fn get_alias_value(&self, table_alias: &str, target: &str) -> Result<&'a Value> {
        let get_value = || {
            if self.table_alias != table_alias {
                return None;
            }

            self.columns
                .iter()
                .position(|column| column.value == target)
                .and_then(|index| self.row.get_value(index))
        };

        match get_value() {
            None => match self.next {
                None => Err(FilterContextError::ValueNotFound.into()),
                Some(context) => context.get_alias_value(table_alias, target),
            },
            Some(value) => Ok(value),
        }
    }

    /*
    pub fn get_value(&self, target: &'a Column) -> Result<&'a Value> {
        let Table { alias, name } = self.table;

        let get_value = || {
            self.columns
                .iter()
                .position(|column| column.name == target.name)
                .and_then(|index| self.row.get_value(index))
                .ok_or_else(|| FilterContextError::ValueNotFound.into())
        };

        match target.table {
            None => get_value(),
            Some(ref table) => {
                if &target.table == alias || table == name {
                    get_value()
                } else {
                    self.next
                        .map(|c| c.get_value(target))
                        .transpose()?
                        .ok_or_else(|| FilterContextError::ValueNotFound.into())
                }
            }
        }
    }
    */
}
