use crate::translator::Row;
use nom_sql::{Column, FieldValueExpression, Literal, LiteralExpression};
use std::fmt::Debug;

fn copy(literal: Literal, literal_expr: &FieldValueExpression) -> Literal {
    let field_literal = match literal_expr {
        FieldValueExpression::Literal(LiteralExpression { value, .. }) => value,
        _ => panic!("[Update->copy_literal] Err on parsing LiteralExpression"),
    };

    match (literal, field_literal) {
        (Literal::Integer(_), &Literal::Integer(v)) => Literal::Integer(v),
        _ => panic!("[Update->copy_literal] Err on parsing literal"),
    }
}

pub struct Update {
    fields: Vec<(Column, FieldValueExpression)>,
}

impl Update {
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
                Some((_, literal_expr)) => (item.0, copy(item.1, literal_expr)),
                None => item,
            })
            .collect::<Vec<(Column, Literal)>>();

        Row { key, items }
    }
}

impl From<Vec<(Column, FieldValueExpression)>> for Update {
    fn from(fields: Vec<(Column, FieldValueExpression)>) -> Self {
        Update { fields }
    }
}
