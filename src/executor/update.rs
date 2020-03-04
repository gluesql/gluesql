use crate::data::{Row, Value};
use nom_sql::{Column, FieldValueExpression, LiteralExpression};
use std::fmt::Debug;

fn copy(
    value: Value,
    (_, literal_expr): &(Column, FieldValueExpression),
) -> Value {
    let field_literal = match literal_expr {
        FieldValueExpression::Literal(LiteralExpression { value, .. }) => value,
        _ => panic!("[Update->copy_literal] Err on parsing LiteralExpression"),
    };

    Value::from((value, field_literal))
}

pub struct Update<'a> {
    pub fields: &'a Vec<(Column, FieldValueExpression)>,
}

impl Update<'_> {
    fn find(&self, column: &Column) -> Option<&(Column, FieldValueExpression)> {
        self.fields
            .iter()
            .filter(|(field_column, _)| column.name == field_column.name)
            .nth(0)
    }

    pub fn apply<T: Debug>(&self, columns: &Vec<Column>, row: Row<T>) -> Row<T> {
        let Row { key, items } = row;
        let items = items
            .into_iter()
            .enumerate()
            .map(|(i, item)| match self.find(&columns[i]) {
                Some(field_item) => copy(item, field_item),
                None => item,
            })
            .collect::<Vec<Value>>();

        Row { key, items }
    }
}
