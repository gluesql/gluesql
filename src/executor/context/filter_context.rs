use std::fmt::Debug;
use std::rc::Rc;

use sqlparser::ast::Ident;

use super::BlendContext;
use crate::data::{Row, Value};

#[derive(Debug)]
pub struct FilterContext<'a> {
    table_alias: &'a str,
    columns: Rc<Vec<Ident>>,
    row: Option<&'a Row>,
    next: Option<Rc<FilterContext<'a>>>,
    next2: Option<Rc<BlendContext<'a>>>,
}

impl<'a> FilterContext<'a> {
    pub fn new(
        table_alias: &'a str,
        columns: Rc<Vec<Ident>>,
        row: Option<&'a Row>,
        next: Option<Rc<FilterContext<'a>>>,
    ) -> Self {
        Self {
            table_alias,
            columns,
            row,
            next,
            next2: None,
        }
    }

    pub fn concat(
        filter_context: Option<Rc<FilterContext<'a>>>,
        blend_context: Option<Rc<BlendContext<'a>>>,
    ) -> Self {
        // TODO: ASAP
        Self {
            table_alias: "this-should-be-fixed-it-is-just-for-temporary-use",
            columns: Rc::new(vec![]),
            row: None,
            next: filter_context,
            next2: blend_context,
        }
    }

    pub fn get_value(&'a self, target: &str) -> Option<&'a Value> {
        let value = self
            .columns
            .iter()
            .position(|column| column.value == target)
            .map(|index| self.row.and_then(|row| row.get_value(index)));

        if let Some(value) = value {
            return value;
        }

        match (&self.next, &self.next2) {
            (None, None) => None,
            (Some(fc), None) => fc.get_value(target),
            (None, Some(bc)) => bc.get_value(target),
            (Some(fc), Some(bc)) => match bc.get_value(target) {
                v @ Some(_) => v,
                None => fc.get_value(target),
            },
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
                .map(|index| self.row.and_then(|row| row.get_value(index)))
        };

        if let Some(value) = get_value() {
            return value;
        }

        match (&self.next, &self.next2) {
            (None, None) => None,
            (Some(fc), None) => fc.get_alias_value(table_alias, target),
            (None, Some(bc)) => bc.get_alias_value(table_alias, target),
            (Some(fc), Some(bc)) => match bc.get_alias_value(table_alias, target) {
                v @ Some(_) => v,
                None => fc.get_alias_value(table_alias, target),
            },
        }
    }
}
