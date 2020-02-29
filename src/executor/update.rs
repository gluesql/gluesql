use crate::data::{Row, Value};
use nom_sql::{Column, FieldValueExpression, LiteralExpression};
use std::fmt::Debug;

fn copy(
    (column, value): (Column, Value),
    (_, literal_expr): &(Column, FieldValueExpression),
) -> (Column, Value) {
    let field_literal = match literal_expr {
        FieldValueExpression::Literal(LiteralExpression { value, .. }) => value,
        _ => panic!("[Update->copy_literal] Err on parsing LiteralExpression"),
    };

    let value = Value::from((value, field_literal));

    (column, value)
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

    pub fn apply<T: Debug>(&self, row: Row<T>) -> Row<T> {
        let Row { key, items } = row;
        let items = items
            .into_iter()
            .map(|item| match self.find(&item.0) {
                Some(field_item) => copy(item, field_item),
                None => item,
            })
            .collect::<Vec<(Column, Value)>>();

        Row { key, items }
    }
}
