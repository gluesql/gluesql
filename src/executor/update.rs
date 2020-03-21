use crate::data::{Row, Value};
use nom_sql::{Column, FieldValueExpression, LiteralExpression};

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

impl<'a> Update<'a> {
    pub fn new(fields: &'a Vec<(Column, FieldValueExpression)>) -> Self {
        Self { fields }
    }

    fn find(&self, column: &Column) -> Option<&(Column, FieldValueExpression)> {
        self.fields
            .iter()
            .filter(|(field_column, _)| column.name == field_column.name)
            .nth(0)
    }

    pub fn apply(&self, columns: &Vec<Column>, row: Row) -> Row {
        let Row(items) = row;
        let items = items
            .into_iter()
            .enumerate()
            .map(|(i, item)| match self.find(&columns[i]) {
                Some(field_item) => copy(item, field_item),
                None => item,
            })
            .collect::<Vec<Value>>();

        Row(items)
    }
}
