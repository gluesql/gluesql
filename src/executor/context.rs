use crate::data::{Row, Value};
use nom_sql::{Column, Table};
use std::convert::From;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Context<'a> {
    table: &'a Table,
    columns: &'a Vec<Column>,
    row: &'a Row,
    next: Option<&'a Context<'a>>,
}

impl<'a> From<(&'a Table, &'a Vec<Column>, &'a Row, Option<&'a Context<'a>>)> for Context<'a> {
    fn from(
        (table, columns, row, next): (&'a Table, &'a Vec<Column>, &'a Row, Option<&'a Context<'a>>),
    ) -> Self {
        Context {
            table,
            columns,
            row,
            next,
        }
    }
}

impl<'a> Context<'a> {
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
