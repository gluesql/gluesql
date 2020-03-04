use crate::data::{Row, Value};
use nom_sql::{Column, Table};
use std::fmt::Debug;

#[derive(Debug)]
pub struct Context<'a, T: Debug> {
    pub table: &'a Table,
    pub columns: &'a Vec<Column>,
    pub row: &'a Row<T>,
    pub next: Option<&'a Context<'a, T>>,
}

impl<'a, T: Debug> Context<'a, T> {
    pub fn get_value(&self, target: &'a Column) -> Option<&'a Value> {
        let Table { alias, name } = self.table;

        let get_value = || {
            let index = self
                .columns
                .iter()
                .position(|column| column.name == target.name)
                .unwrap();

            self.row.get_value(index)
        };

        match target.table {
            None => get_value(),
            Some(ref table) => {
                if &target.table == alias || table == name {
                    get_value()
                } else {
                    self.next.unwrap().get_value(target)
                }
            }
        }
    }
}
