use iter_enum::Iterator;
use nom_sql::FieldDefinitionExpression;
use std::fmt::Debug;
use std::iter::once;
use std::rc::Rc;
use thiserror::Error;

use crate::data::Row;
use crate::executor::BlendContext;
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum BlendError {
    #[error("this field definition is not supported yet")]
    FieldDefinitionNotSupported,

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("table not found: {0}")]
    TableNotFound(String),
}

#[derive(Iterator)]
enum Blended<I1, I2, I3, I4> {
    All(I1),
    AllInTable(I2),
    Col(I3),
    Err(I4),
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
        macro_rules! err {
            ($err: expr) => {
                Blended::Err(once(Err($err.into())))
            };
        }

        self.fields
            .iter()
            .flat_map(|expr| match expr {
                FieldDefinitionExpression::All => {
                    Blended::All(context.get_values().into_iter().map(Ok))
                }
                FieldDefinitionExpression::AllInTable(table_name) => {
                    match context.get_table_values(&table_name) {
                        Some(values) => Blended::AllInTable(values.into_iter().map(Ok)),
                        None => err!(BlendError::TableNotFound(table_name.clone())),
                    }
                }
                FieldDefinitionExpression::Col(column) => match context.get_value(column) {
                    Some(value) => Blended::Col(once(Ok(value))),
                    None => err!(BlendError::ColumnNotFound(column.name.clone())),
                },
                FieldDefinitionExpression::Value(_) => {
                    err!(BlendError::FieldDefinitionNotSupported)
                }
            })
            .collect::<Result<_>>()
            .map(Row)
    }
}
