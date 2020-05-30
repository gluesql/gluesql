use nom_sql::{Column, Table};
use std::fmt::Debug;
use std::rc::Rc;

use crate::data::{Row, Value};

#[derive(Debug)]
pub struct BlendContext<'a, T: 'static + Debug> {
    pub table: &'a Table,
    pub columns: &'a [Column],
    pub key: T,
    pub row: Row,
    pub next: Option<Rc<BlendContext<'a, T>>>,
}

impl<'a, T: 'static + Debug> BlendContext<'a, T> {
    pub fn get_value(&self, target: &Column) -> Option<Value> {
        let Table { alias, name } = self.table;

        let get_value = || {
            self.columns
                .iter()
                .position(|column| column.name == target.name)
                .and_then(|index| self.row.get_value(index))
                .cloned()
        };

        match target.table {
            None => get_value(),
            Some(ref table) => {
                if &target.table == alias || table == name {
                    get_value()
                } else {
                    self.next.as_ref().and_then(|c| c.get_value(target))
                }
            }
        }
    }

    pub fn get_values(&self) -> Vec<Value> {
        let Row(values) = &self.row;
        let values = values.clone();

        match &self.next {
            Some(context) => values
                .into_iter()
                .chain(context.get_values().into_iter())
                .collect(),
            None => values,
        }
    }

    pub fn get_table_values(&self, table_name: &str) -> Option<Vec<Value>> {
        let Table { alias, name } = self.table;

        if table_name == alias.as_ref().unwrap_or(name) {
            let Row(values) = &self.row;

            Some(values.clone())
        } else {
            self.next
                .as_ref()
                .and_then(|context| context.get_table_values(table_name))
        }
    }
}
