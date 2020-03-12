use crate::data::{Row, Value};
use nom_sql::{Column, Table};
use std::fmt::Debug;
use std::rc::Rc;

#[derive(Debug)]
pub struct BlendContext<'a, T: 'static + Debug> {
    pub table: &'a Table,
    pub columns: Rc<Vec<Column>>,
    pub key: T,
    pub row: Row,
    pub next: Option<Box<BlendContext<'a, T>>>,
}

impl<'a, T: 'static + Debug> BlendContext<'a, T> {
    pub fn get_value(&'a self, target: &'a Column) -> Option<&'a Value> {
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
                    match self.next.as_ref() {
                        Some(c) => c.get_value(target),
                        None => None,
                    }
                }
            }
        }
    }
}
