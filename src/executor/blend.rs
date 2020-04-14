use nom_sql::{Column, FieldDefinitionExpression};
use std::fmt::Debug;
use thiserror::Error;

use crate::data::{Row, Value};
use crate::executor::BlendContext;
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum BlendError {
    #[error("this field definition is not supported yet")]
    FieldDefinitionNotSupported,
}

pub struct Blend<'a> {
    fields: &'a Vec<FieldDefinitionExpression>,
}

impl<'a> Blend<'a> {
    pub fn new(fields: &'a Vec<FieldDefinitionExpression>) -> Self {
        Self { fields }
    }

    pub fn apply<T: 'static + Clone + Debug>(
        &self,
        blend_context: Result<BlendContext<'a, T>>,
    ) -> Result<Row> {
        let BlendContext { columns, row, .. } = blend_context?;

        // TODO: Should support JOIN
        self.blend(&columns, row)
    }

    fn blend(&self, columns: &Vec<Column>, row: Row) -> Result<Row> {
        let Row(values) = row;
        let values = values
            .into_iter()
            .zip(columns.iter())
            .filter_map(|(value, column)| self.find(value, column))
            .collect::<Result<_>>()?;

        Ok(Row(values))
    }

    fn find(&self, value: Value, target: &Column) -> Option<Result<Value>> {
        for expr in self.fields {
            match expr {
                FieldDefinitionExpression::All => {
                    return Some(Ok(value));
                }
                FieldDefinitionExpression::Col(column) => {
                    if column.name == target.name {
                        return Some(Ok(value));
                    }
                }
                FieldDefinitionExpression::AllInTable(_) | FieldDefinitionExpression::Value(_) => {
                    return Some(Err(BlendError::FieldDefinitionNotSupported.into()));
                }
            }
        }

        None
    }
}
