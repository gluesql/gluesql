use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::Ident;

use super::FilterContext;
use crate::data::{Row, Value};
use crate::result::Result;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum BlendContextError {
    #[error("value not found")]
    ValueNotFound,
}

#[derive(Debug)]
pub struct BlendContext<'a> {
    table_alias: &'a str,
    columns: Rc<Vec<Ident>>,
    row: Option<Row>,
    next: Option<Rc<BlendContext<'a>>>,
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

    pub fn concat_into(
        &'a self,
        filter_context: Option<Rc<FilterContext<'a>>>,
    ) -> Option<Rc<FilterContext<'a>>> {
        let BlendContext {
            table_alias,
            columns,
            row,
            next,
            ..
        } = self;

        let filter_context =
            FilterContext::new(table_alias, &columns, row.as_ref(), filter_context);
        let filter_context = Some(Rc::new(filter_context));

        match next {
            Some(next) => next.concat_into(filter_context),
            None => filter_context,
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

    pub fn get_alias_values(&self, alias: &str) -> Option<Vec<Value>> {
        if self.table_alias == alias {
            let values = match &self.row {
                Some(Row(values)) => values.clone(),
                None => self.columns.iter().map(|_| Value::Empty).collect(),
            };

            Some(values)
        } else {
            self.next
                .as_ref()
                .and_then(|next| next.get_alias_values(alias))
        }
    }

    pub fn get_all_values(&'a self) -> Vec<Value> {
        let values: Vec<Value> = match &self.row {
            Some(Row(values)) => values.clone(),
            None => self.columns.iter().map(|_| Value::Empty).collect(),
        };

        match &self.next {
            Some(next) => next
                .get_all_values()
                .into_iter()
                .chain(values.into_iter())
                .collect(),
            None => values,
        }
    }
}
