// use iter_enum::Iterator;
// use nom_sql::{Column, FieldDefinitionExpression, Table};
use std::fmt::Debug;
// use std::iter::once;
// use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Expr, SelectItem};

// use crate::data::{Row, Value};
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

pub struct Blend<'a> {
    fields: &'a [SelectItem],
}

impl<'a> Blend<'a> {
    pub fn new(fields: &'a [SelectItem]) -> Self {
        Self { fields }
    }

    pub fn apply<T: 'static + Debug>(
        &self,
        // context: Result<Rc<BlendContext<'a, T>>>,
        context: Result<BlendContext<'a, T>>,
    ) -> Result<Row> {
        let BlendContext {
            row: Row(values),
            columns,
            ..
        } = context?;

        let values = values
            .into_iter()
            .zip(columns.iter())
            .filter_map(|(value, column)| {
                self.fields
                    .iter()
                    .find_map(|field| match field {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            Expr::Identifier(ident) => {
                                if column.value == ident.value {
                                    Some(Ok(()))
                                } else {
                                    None
                                }
                            }
                            _ => Some(Err(BlendError::FieldDefinitionNotSupported.into())),
                        },
                        SelectItem::Wildcard => Some(Ok(())),
                        _ => None,
                    })
                    .map(|v| v.map(|()| value))
            })
            .collect::<Result<_>>()?;

        Ok(Row(values))
    }
}

/*
#[derive(Iterator)]
enum Blended<I1, I2, I3, I4> {
    All(I1),
    AllInTable(I2),
    Col(I3),
    Err(I4),
}
*/

/*
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
                    Blended::All(get_values(&context).into_iter().map(Ok))
                }
                FieldDefinitionExpression::AllInTable(table_name) => {
                    match get_table_values(&context, &table_name) {
                        Some(values) => Blended::AllInTable(values.into_iter().map(Ok)),
                        None => err!(BlendError::TableNotFound(table_name.clone())),
                    }
                }
                FieldDefinitionExpression::Col(column) => match get_value(&context, column) {
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

fn get_value<T: 'static + Debug>(context: &BlendContext<T>, target: &Column) -> Option<Value> {
    let Table { alias, name } = context.table;

    let get = || {
        context
            .columns
            .iter()
            .position(|column| column.name == target.name)
            .and_then(|index| context.row.get_value(index))
            .cloned()
    };

    match target.table {
        None => get(),
        Some(ref table) => {
            if &target.table == alias || table == name {
                get()
            } else {
                context
                    .next
                    .as_ref()
                    .and_then(|next| get_value(next, target))
            }
        }
    }
}

fn get_values<T: 'static + Debug>(context: &BlendContext<T>) -> Vec<Value> {
    let Row(values) = &context.row;
    let values = values.clone();

    match &context.next {
        Some(context) => values
            .into_iter()
            .chain(get_values(&context).into_iter())
            .collect(),
        None => values,
    }
}

fn get_table_values<T: 'static + Debug>(
    context: &BlendContext<T>,
    table_name: &str,
) -> Option<Vec<Value>> {
    let Table { alias, name } = &context.table;

    if table_name == alias.as_ref().unwrap_or(name) {
        let Row(values) = &context.row;

        Some(values.clone())
    } else {
        context
            .next
            .as_ref()
            .and_then(|context| get_table_values(context, table_name))
    }
}
*/
