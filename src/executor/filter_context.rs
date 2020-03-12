use crate::data::{Row, Value};
use nom_sql::{Column, Table};
use std::convert::From;
use std::fmt::Debug;

#[derive(Debug)]
pub struct FilterContext<'a> {
    table: &'a Table,
    columns: &'a Vec<Column>,
    row: &'a Row,
    next: Option<&'a FilterContext<'a>>,
}

impl<'a>
    From<(
        &'a Table,
        &'a Vec<Column>,
        &'a Row,
        Option<&'a FilterContext<'a>>,
    )> for FilterContext<'a>
{
    fn from(
        (table, columns, row, next): (
            &'a Table,
            &'a Vec<Column>,
            &'a Row,
            Option<&'a FilterContext<'a>>,
        ),
    ) -> Self {
        FilterContext {
            table,
            columns,
            row,
            next,
        }
    }
}

impl<'a> FilterContext<'a> {
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
                    self.next.and_then(|c| c.get_value(target))
                }
            }
        }
    }
}
