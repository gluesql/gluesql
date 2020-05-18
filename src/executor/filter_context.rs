use nom_sql::{Column, Table};
use std::fmt::Debug;
use thiserror::Error;

use crate::data::{Row, Value};
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum FilterContextError {
    #[error("value not found")]
    ValueNotFound,
}

#[derive(Debug)]
pub struct FilterContext<'a> {
    table: &'a Table,
    columns: &'a [Column],
    row: &'a Row,
    next: Option<&'a FilterContext<'a>>,
}

impl<'a> FilterContext<'a> {
    pub fn new(
        table: &'a Table,
        columns: &'a [Column],
        row: &'a Row,
        next: Option<&'a FilterContext<'a>>,
    ) -> Self {
        Self {
            table,
            columns,
            row,
            next,
        }
    }

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
}
