use iter_enum::Iterator;
use std::fmt::Debug;
use std::iter::once;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Expr, Ident, SelectItem};

use crate::data::{get_table_name, Row, Value};
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

#[derive(Iterator)]
enum Blended<I1, I2, I3, I4> {
    All(I1),
    AllInTable(I2),
    Single(I3),
    Err(I4),
}

struct Context<'a> {
    table_alias: &'a str,
    columns: Rc<Vec<Ident>>,
    values: Vec<Rc<Value>>,
    next: Option<Box<Context<'a>>>,
}

impl<'a> Blend<'a> {
    pub fn new(fields: &'a [SelectItem]) -> Self {
        Self { fields }
    }

    pub fn apply<T: 'static + Debug>(
        &self,
        context: Result<Rc<BlendContext<'a, T>>>,
    ) -> Result<Row> {
        let context = Self::prepare(context?);

        let values = self
            .blend(context)?
            .into_iter()
            .map(|value| match Rc::try_unwrap(value) {
                Ok(value) => value,
                Err(value) => (*value).clone(),
            })
            .collect();

        Ok(Row(values))
    }

    fn prepare<T: 'static + Debug>(context: Rc<BlendContext<'a, T>>) -> Context<'a> {
        match Rc::try_unwrap(context) {
            Ok(BlendContext {
                table_alias,
                columns,
                row,
                next,
                ..
            }) => {
                let Row(values) = row;
                let values = values.into_iter().map(Rc::new).collect();
                let next = next.map(|c| Box::new(Self::prepare(c)));

                Context {
                    table_alias,
                    columns,
                    values,
                    next,
                }
            }
            Err(context) => {
                let BlendContext {
                    table_alias,
                    columns,
                    row,
                    next,
                    ..
                } = &(*context);

                let columns = Rc::clone(columns);
                let Row(values) = row;
                let values = values.clone().into_iter().map(Rc::new).collect();
                let next = next.as_ref().map(|c| {
                    let c = Rc::clone(c);
                    let c = Self::prepare(c);

                    Box::new(c)
                });

                Context {
                    table_alias,
                    columns,
                    values,
                    next,
                }
            }
        }
    }

    fn blend(&self, context: Context<'a>) -> Result<Vec<Rc<Value>>> {
        macro_rules! err {
            ($err: expr) => {
                Blended::Err(once(Err($err.into())))
            };
        }

        self.fields
            .iter()
            .flat_map(|item| match item {
                SelectItem::Wildcard => {
                    let values = get_all_values(&context).into_iter().map(Ok);

                    Blended::All(values)
                }
                SelectItem::QualifiedWildcard(alias) => {
                    let table_alias = match get_table_name(alias) {
                        Ok(alias) => alias,
                        Err(e) => {
                            return err!(e);
                        }
                    };

                    match get_alias_values(&context, table_alias) {
                        Some(values) => Blended::AllInTable(values.into_iter().map(Ok)),
                        None => err!(BlendError::TableNotFound(table_alias.to_string())),
                    }
                }
                SelectItem::UnnamedExpr(expr) => match expr {
                    Expr::Identifier(ident) => match get_value(&context, &ident.value) {
                        Some(value) => Blended::Single(once(Ok(value))),
                        None => err!(BlendError::ColumnNotFound(ident.to_string())),
                    },
                    Expr::CompoundIdentifier(idents) => {
                        if idents.len() != 2 {
                            return err!(BlendError::FieldDefinitionNotSupported);
                        }

                        let table_alias = &idents[0].value;
                        let column = &idents[1].value;

                        match get_alias_value(&context, table_alias, column) {
                            Some(value) => Blended::Single(once(Ok(value))),
                            None => err!(BlendError::ColumnNotFound(format!(
                                "{}.{}",
                                table_alias, column
                            ))),
                        }
                    }
                    _ => err!(BlendError::FieldDefinitionNotSupported),
                },
                SelectItem::ExprWithAlias { .. } => err!(BlendError::FieldDefinitionNotSupported),
            })
            .collect::<Result<_>>()
    }
}

fn get_value(context: &Context<'_>, target: &str) -> Option<Rc<Value>> {
    let Context {
        values,
        columns,
        next,
        ..
    } = context;

    columns
        .iter()
        .position(|column| column.value == target)
        .map(|index| Rc::clone(&values[index]))
        .or_else(|| next.as_ref().and_then(|next| get_value(next, target)))
}

fn get_alias_value(context: &Context<'_>, alias: &str, target: &str) -> Option<Rc<Value>> {
    let Context {
        table_alias,
        values,
        columns,
        next,
    } = context;

    if table_alias == &alias {
        columns
            .iter()
            .position(|column| column.value == target)
            .map(|index| Rc::clone(&values[index]))
    } else {
        next.as_ref()
            .and_then(|next| get_alias_value(next, alias, target))
    }
}

fn get_all_values(context: &Context<'_>) -> Vec<Rc<Value>> {
    let values = context.values.iter().map(Rc::clone);

    match &context.next {
        Some(context) => values.chain(get_all_values(&context).into_iter()).collect(),
        None => values.collect(),
    }
}

fn get_alias_values(context: &Context<'_>, table_alias: &str) -> Option<Vec<Rc<Value>>> {
    if table_alias == context.table_alias {
        let values = context.values.iter().map(Rc::clone);

        Some(values.collect())
    } else {
        context
            .next
            .as_ref()
            .and_then(|c| get_alias_values(c, table_alias))
    }
}
