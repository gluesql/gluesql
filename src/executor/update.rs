use nom_sql::{
    ArithmeticBase, ArithmeticOperator, Column, FieldValueExpression, LiteralExpression,
};
use std::collections::HashMap;
use thiserror::Error;

use crate::data::{Row, Value};
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum UpdateError {
    #[error("column not found {0}")]
    ColumnNotFound(String),

    #[error("conflict on schema, {0} does not exist")]
    ConflictOnSchema(String),
}

pub struct Update<'a> {
    fields: &'a Vec<(Column, FieldValueExpression)>,
}

impl<'a> Update<'a> {
    pub fn new(fields: &'a Vec<(Column, FieldValueExpression)>) -> Self {
        Self { fields }
    }

    pub fn apply(&self, columns: &Vec<Column>, row: Row) -> Result<Row> {
        let Row(values) = row;
        let values_map = columns
            .iter()
            .map(|c| &c.name)
            .zip(values.iter())
            .collect::<HashMap<_, _>>();

        self.fields.iter().try_for_each(|(column, _)| {
            let name = &column.name;

            match values_map.contains_key(name) {
                true => Ok(()),
                false => Err(UpdateError::ColumnNotFound(name.clone())),
            }
        })?;

        let field_values = columns
            .iter()
            .map(|column| match self.find(column) {
                Some(expr) => self.evaluate(column, expr, &values_map).map(Some),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;

        let values = values
            .into_iter()
            .zip(field_values.into_iter())
            .map(move |(value, field_value)| match field_value {
                Some(field_value) => field_value,
                None => value,
            })
            .collect::<Vec<Value>>();

        Ok(Row(values))
    }

    fn find(&self, column: &Column) -> Option<&FieldValueExpression> {
        self.fields
            .iter()
            .find(|(field_column, _)| column.name == field_column.name)
            .map(|(_, expr)| expr)
    }

    fn evaluate(
        &self,
        column: &Column,
        expr: &FieldValueExpression,
        values_map: &HashMap<&String, &Value>,
    ) -> Result<Value> {
        match expr {
            FieldValueExpression::Literal(LiteralExpression {
                value: field_literal,
                ..
            }) => {
                let name = &column.name;
                let value = values_map
                    .get(name)
                    .ok_or(UpdateError::ConflictOnSchema(name.clone()))?;

                value.clone_by(field_literal)
            }
            FieldValueExpression::Arithmetic(expr) => {
                let parse_base = |base: &ArithmeticBase| -> Result<Value> {
                    match base {
                        ArithmeticBase::Column(field_column) => {
                            let name = &field_column.name;
                            let value = values_map
                                .get(name)
                                .ok_or(UpdateError::ColumnNotFound(name.clone()))?;

                            Ok(value.clone().to_owned())
                        }
                        ArithmeticBase::Scalar(literal) => {
                            let name = &column.name;
                            let value = values_map
                                .get(name)
                                .ok_or(UpdateError::ConflictOnSchema(name.clone()))?;

                            value.clone_by(&literal)
                        }
                    }
                };

                let l = parse_base(&expr.left)?;
                let r = parse_base(&expr.right)?;

                match expr.op {
                    ArithmeticOperator::Add => Ok(l.add(&r)?),
                    ArithmeticOperator::Subtract => Ok(l.subtract(&r)?),
                    ArithmeticOperator::Multiply => Ok(l.multiply(&r)?),
                    ArithmeticOperator::Divide => Ok(l.divide(&r)?),
                }
            }
        }
    }
}
