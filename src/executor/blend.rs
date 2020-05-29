use nom_sql::{Column, FieldDefinitionExpression};
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use crate::data::{Row, Value};
use crate::executor::BlendContext;
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum BlendError {
    #[error("this field definition is not supported yet")]
    FieldDefinitionNotSupported,
}

enum Param<'a> {
    Val(Value),
    Ref(&'a Value),
}

impl<'a> Into<Value> for Param<'a> {
    fn into(self) -> Value {
        match self {
            Param::Val(value) => value,
            Param::Ref(value) => value.clone(),
        }
    }
}

pub struct Blend<'a> {
    fields: &'a [FieldDefinitionExpression],
}

impl<'a> Blend<'a> {
    pub fn new(fields: &'a [FieldDefinitionExpression]) -> Self {
        Self { fields }
    }

    pub fn apply<T: 'static + Debug>(
        &self,
        context: Result<Rc<BlendContext<'a, T>>>,
    ) -> Result<Row> {
        self.blend(context?)
    }

    fn blend<T: 'static + Debug>(&self, context: Rc<BlendContext<'a, T>>) -> Result<Row> {
        macro_rules! blend {
            ($values: expr, $columns: expr) => {
                $values
                    .zip($columns.iter())
                    .filter_map(|(value, column)| self.find(column, value))
                    .collect::<Result<_>>()
                    .map(Row)
            };
        }

        match Rc::try_unwrap(context) {
            Ok(context) => {
                let BlendContext { columns, row, .. } = context;

                let Row(values) = row;
                let values = values.into_iter().map(Param::Val);

                blend!(values, columns)
            }
            Err(context) => {
                let &BlendContext {
                    columns, ref row, ..
                } = &(*context);

                let Row(values) = row;
                let values = values.iter().map(|v| Param::Ref(&v));

                blend!(values, columns)
            }
        }
    }

    fn find(&self, target: &Column, value: Param) -> Option<Result<Value>> {
        for expr in self.fields {
            match expr {
                FieldDefinitionExpression::All => {
                    return Some(Ok(value.into()));
                }
                FieldDefinitionExpression::Col(column) => {
                    if column.name == target.name {
                        return Some(Ok(value.into()));
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
