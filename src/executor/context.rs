use crate::row::Row;
use nom_sql::{Column, Literal, Table};
use std::fmt::Debug;

#[derive(Debug)]
pub struct Context<'a, T: Debug> {
    pub table: &'a Table,
    pub row: &'a Row<T>,
    pub next: Option<&'a Context<'a, T>>,
}

impl<'a, T: Debug> Context<'a, T> {
    pub fn get_literal(&self, column: &'a Column) -> Option<&'a Literal> {
        let Table { alias, name } = self.table;

        match column.table {
            None => self.row.get_literal(column),
            Some(ref table) => {
                if &column.table == alias || table == name {
                    self.row.get_literal(column)
                } else {
                    self.next.unwrap().get_literal(column)
                }
            }
        }
    }
}
