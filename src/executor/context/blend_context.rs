use std::fmt::Debug;
use std::rc::Rc;

use sqlparser::ast::Ident;

use crate::data::{Row, Value};

#[derive(Debug)]
pub struct BlendContext<'a> {
    table_alias: &'a str,
    pub columns: Rc<[Ident]>,
    pub row: Option<Row>,
    next: Option<Rc<BlendContext<'a>>>,
}

impl<'a> BlendContext<'a> {
    pub fn new(
        table_alias: &'a str,
        columns: Rc<[Ident]>,
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

    pub fn get_value(&'a self, target: &str) -> Option<&'a Value> {
        let get_value = || {
            self.columns
                .iter()
                .position(|column| column.value == target)
                .map(|index| self.row.as_ref().and_then(|row| row.get_value(index)))
        };

        match get_value() {
            None => match &self.next {
                None => None,
                Some(context) => context.get_value(target),
            },
            Some(value) => value,
        }
    }

    pub fn get_alias_value(&'a self, table_alias: &str, target: &str) -> Option<&'a Value> {
        let get_value = || {
            if self.table_alias != table_alias {
                return None;
            }

            self.columns
                .iter()
                .position(|column| column.value == target)
                .map(|index| self.row.as_ref().and_then(|row| row.get_value(index)))
        };

        match get_value() {
            None => match &self.next {
                None => None,
                Some(context) => context.get_alias_value(table_alias, target),
            },
            Some(value) => value,
        }
    }

    pub fn get_alias_values(&self, alias: &str) -> Option<Vec<Value>> {
        if self.table_alias == alias {
            let values = match &self.row {
                Some(Row(values)) => values.clone(),
                None => self.columns.iter().map(|_| Value::Null).collect(),
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
            None => self.columns.iter().map(|_| Value::Null).collect(),
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
