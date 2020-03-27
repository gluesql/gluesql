use nom_sql::{Column, FieldDefinitionExpression};
use std::fmt::Debug;
use thiserror::Error;

use crate::{BlendContext, Result, Row, Value};

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

    pub fn apply<T: 'static + Debug>(
        &self,
        blend_context: Result<BlendContext<'a, T>>,
    ) -> Result<Row> {
        let BlendContext { columns, row, .. } = blend_context?;

        // TODO: Should support JOIN
        self.blend(&columns, row)
    }

    fn blend(&self, columns: &Vec<Column>, row: Row) -> Result<Row> {
        let Row(items) = row;
        let items = items
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| self.check(&columns, v, i))
            .collect::<Result<_>>()?;

        Ok(Row(items))
    }

    fn check(&self, columns: &Vec<Column>, value: Value, index: usize) -> Option<Result<Value>> {
        for expr in self.fields {
            match expr {
                FieldDefinitionExpression::All => {
                    return Some(Ok(value));
                }
                FieldDefinitionExpression::Col(column) => {
                    if column.name == columns[index].name {
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
