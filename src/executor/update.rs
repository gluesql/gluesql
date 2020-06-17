use std::collections::HashMap;
use thiserror::Error;

use sqlparser::ast::{Assignment, Expr, Ident};

use crate::data::{Row, Value};
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum UpdateError {
    #[error("column not found {0}")]
    ColumnNotFound(String),

    #[error("expression not supported {0}")]
    ExpressionNotSupported(String),

    #[error("conflict on schema, {0} does not exist")]
    ConflictOnSchema(String),
}

pub struct Update<'a> {
    fields: &'a [Assignment],
}

impl<'a> Update<'a> {
    pub fn new(fields: &'a [Assignment]) -> Self {
        Self { fields }
    }

    pub fn apply(&self, columns: &[Ident], row: Row) -> Result<Row> {
        let Row(values) = row;
        let values_map = columns
            .iter()
            .map(|c| &c.value)
            .zip(values.iter())
            .collect::<HashMap<_, _>>();

        self.fields.iter().try_for_each(|assignment| {
            let name = &assignment.id.value;

            if values_map.contains_key(name) {
                Ok(())
            } else {
                Err(UpdateError::ColumnNotFound(name.clone()))
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

    fn find(&self, column: &Ident) -> Option<&Expr> {
        self.fields
            .iter()
            .find(|assignment| column.value == assignment.id.value)
            .map(|assignment| &assignment.value)
    }

    fn evaluate(
        &self,
        column: &Ident,
        expr: &Expr,
        values_map: &HashMap<&String, &Value>,
    ) -> Result<Value> {
        match expr {
            Expr::Value(ast_value) => {
                let name = &column.value;
                let value = values_map
                    .get(name)
                    .ok_or_else(|| UpdateError::ConflictOnSchema(name.clone()))?;

                value.clone_by(ast_value)
            }
            _ => Err(UpdateError::ExpressionNotSupported(column.value.clone()).into()),
        }

        /*
        match expr {
            FieldValueExpression::Arithmetic(expr) => {
                let parse_base = |base: &ArithmeticBase| -> Result<Value> {
                    match base {
                        ArithmeticBase::Column(field_column) => {
                            let name = &field_column.name;
                            let value = values_map
                                .get(name)
                                .ok_or_else(|| UpdateError::ColumnNotFound(name.clone()))?;

                            Ok((*value).clone())
                        }
                        ArithmeticBase::Scalar(literal) => {
                            let name = &column.name;
                            let value = values_map
                                .get(name)
                                .ok_or_else(|| UpdateError::ConflictOnSchema(name.clone()))?;

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
        */
    }
}
