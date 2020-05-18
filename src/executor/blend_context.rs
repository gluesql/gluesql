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

// TODO: use this when you implement JOIN + blend
impl<'a, T: 'static + Debug> BlendContext<'a, T> {
    pub fn get_value(&'a self, target: &'a Column) -> Option<&'a Value> {
        let Table { alias, name } = self.table;

        let get_value = || {
            self.columns
                .iter()
                .position(|column| column.name == target.name)
                .and_then(|index| self.row.get_value(index))
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
}
