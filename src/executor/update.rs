use crate::data::{Row, Value};
use nom_sql::{Column, FieldValueExpression, LiteralExpression};
use std::convert::From;

fn copy(value: Value, (_, literal_expr): &(Column, FieldValueExpression)) -> Value {
    let field_literal = match literal_expr {
        FieldValueExpression::Literal(LiteralExpression { value, .. }) => value,
        _ => panic!("[Update->copy_literal] Err on parsing LiteralExpression"),
    };

    Value::from((value, field_literal))
}

pub struct Update<'a> {
    fields: &'a Vec<(Column, FieldValueExpression)>,
}

impl<'a> From<&'a Vec<(Column, FieldValueExpression)>> for Update<'a> {
    fn from(fields: &'a Vec<(Column, FieldValueExpression)>) -> Self {
        Update { fields }
    }
}

impl Update<'_> {
    fn find(&self, column: &Column) -> Option<&(Column, FieldValueExpression)> {
        self.fields
            .iter()
            .filter(|(field_column, _)| column.name == field_column.name)
            .nth(0)
    }

    pub fn apply(&self, columns: &Vec<Column>, row: Row) -> Row {
        let Row { items } = row;
        let items = items
            .into_iter()
            .enumerate()
            .map(|(i, item)| match self.find(&columns[i]) {
                Some(field_item) => copy(item, field_item),
                None => item,
            })
            .collect::<Vec<Value>>();

        Row { items }
    }
}
