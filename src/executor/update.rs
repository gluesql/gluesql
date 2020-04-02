use nom_sql::{Column, FieldValueExpression, LiteralExpression};
use thiserror::Error;

use crate::data::{Row, Value};
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum UpdateError {
    #[error("field value expression not supported yet")]
    ExpressionNotSupported,
}

fn copy(value: Value, (_, literal_expr): &(Column, FieldValueExpression)) -> Result<Value> {
    match literal_expr {
        FieldValueExpression::Literal(LiteralExpression {
            value: field_literal,
            ..
        }) => value.clone_by(field_literal),
        _ => Err(UpdateError::ExpressionNotSupported.into()),
    }
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
            .find(|(field_column, _)| column.name == field_column.name)
    }

    pub fn apply(&self, columns: &Vec<Column>, row: Row) -> Result<Row> {
        let Row(items) = row;
        let items = items
            .into_iter()
            .enumerate()
            .map(|(i, item)| match self.find(&columns[i]) {
                Some(field_item) => copy(item, field_item),
                None => Ok(item),
            })
            .collect::<Result<_>>()?;

        Ok(Row(items))
    }
}
