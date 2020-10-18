use im_rc::HashMap;
use iter_enum::Iterator;
use serde::Serialize;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::iter::once;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Expr, Function, Ident, SelectItem};

use super::context::{AggregateContext, BlendContext, FilterContext};
use super::evaluate::{evaluate, Evaluated};
use crate::data::{get_name, Row, Value};
use crate::result::Result;
use crate::store::Store;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum BlendError {
    #[error("Not supported compound identifier: {0}")]
    NotSupportedCompoundIdentifier(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("table not found: {0}")]
    TableNotFound(String),
}

pub struct Blend<'a, T: 'static + Debug> {
    storage: &'a dyn Store<T>,
    fields: &'a [SelectItem],
}

#[derive(Iterator)]
enum Blended<I1, I2, I3, I4> {
    All(I1),
    AllInTable(I2),
    Single(I3),
    Err(I4),
}

impl<'a, T: 'static + Debug> Blend<'a, T> {
    pub fn new(storage: &'a dyn Store<T>, fields: &'a [SelectItem]) -> Self {
        Self { storage, fields }
    }

    pub fn apply(&self, context: Result<AggregateContext<'a>>) -> Result<Row> {
        let AggregateContext { aggregated, next } = context?;
        let context = Self::prepare(next);

        let values = self
            .blend(aggregated, context)?
            .into_iter()
            .map(|value| match Rc::try_unwrap(value) {
                Ok(value) => value,
                Err(value) => (*value).clone(),
            })
            .collect();

        Ok(Row(values))
    }

    fn prepare(context: Rc<BlendContext<'a>>) -> Context<'a> {
        match Rc::try_unwrap(context) {
            Ok(BlendContext {
                table_alias,
                columns,
                row,
                next,
                ..
            }) => {
                let values = row.map(|row| {
                    let Row(values) = row;

                    values.into_iter().map(Rc::new).collect()
                });
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
                let values = row.as_ref().map(|row| {
                    let Row(values) = row;

                    values.clone().into_iter().map(Rc::new).collect()
                });
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

    fn blend(
        &self,
        aggregated: Option<HashMap<&'a Function, Value>>,
        context: Context<'a>,
    ) -> Result<Vec<Rc<Value>>> {
        macro_rules! err {
            ($err: expr) => {
                Blended::Err(once(Err($err.into())))
            };
        }

        self.fields
            .iter()
            .flat_map(|item| match item {
                SelectItem::Wildcard => {
                    let values = context.get_all_values().into_iter().map(Ok);

                    Blended::All(values)
                }
                SelectItem::QualifiedWildcard(alias) => {
                    let table_alias = match get_name(alias) {
                        Ok(alias) => alias,
                        Err(e) => {
                            return err!(e);
                        }
                    };

                    match context.get_alias_values(table_alias) {
                        Some(values) => Blended::AllInTable(values.into_iter().map(Ok)),
                        None => err!(BlendError::TableNotFound(table_alias.to_string())),
                    }
                }
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    match expr {
                        Expr::Identifier(ident) => match context.get_value(&ident.value) {
                            Some(value) => Blended::Single(once(Ok(value))),
                            None => err!(BlendError::ColumnNotFound(ident.to_string())),
                        },
                        Expr::CompoundIdentifier(idents) => {
                            if idents.len() != 2 {
                                return err!(BlendError::NotSupportedCompoundIdentifier(
                                    expr.to_string()
                                ));
                            }

                            let table_alias = &idents[0].value;
                            let column = &idents[1].value;

                            match context.get_alias_value(table_alias, column) {
                                Some(value) => Blended::Single(once(Ok(value))),
                                None => err!(BlendError::ColumnNotFound(format!(
                                    "{}.{}",
                                    table_alias, column
                                ))),
                            }
                        }
                        _ => {
                            let value = evaluate_blended(
                                self.storage,
                                None,
                                &context,
                                aggregated.as_ref(),
                                expr,
                            )
                            .map(Rc::new);

                            Blended::Single(once(value))
                        }
                    }
                }
            })
            .collect::<Result<_>>()
    }
}

struct Context<'a> {
    table_alias: &'a str,
    columns: Rc<Vec<Ident>>,
    values: Option<Vec<Rc<Value>>>,
    next: Option<Box<Context<'a>>>,
}

impl Context<'_> {
    fn get_value(&self, target: &str) -> Option<Rc<Value>> {
        let Context {
            values,
            columns,
            next,
            ..
        } = self;

        columns
            .iter()
            .position(|column| column.value == target)
            .map(|index| match values {
                Some(values) => Rc::clone(&values[index]),
                None => Rc::new(Value::Empty),
            })
            .or_else(|| next.as_ref().and_then(|next| next.get_value(target)))
    }

    fn get_alias_value(&self, alias: &str, target: &str) -> Option<Rc<Value>> {
        let Context {
            table_alias,
            values,
            columns,
            next,
        } = self;

        if table_alias == &alias {
            columns
                .iter()
                .position(|column| column.value == target)
                .map(|index| match values {
                    Some(values) => Rc::clone(&values[index]),
                    None => Rc::new(Value::Empty),
                })
        } else {
            next.as_ref()
                .and_then(|next| next.get_alias_value(alias, target))
        }
    }

    fn get_all_values(&self) -> Vec<Rc<Value>> {
        let Context {
            values,
            next,
            columns,
            ..
        } = self;

        let values: Vec<Rc<Value>> = match values {
            Some(values) => values.iter().map(Rc::clone).collect(),
            None => columns.iter().map(|_| Rc::new(Value::Empty)).collect(),
        };

        match next.as_ref() {
            Some(next) => next
                .get_all_values()
                .into_iter()
                .chain(values.into_iter())
                .collect(),
            None => values,
        }
    }

    fn get_alias_values(&self, alias: &str) -> Option<Vec<Rc<Value>>> {
        let Context {
            table_alias,
            values,
            columns,
            next,
        } = self;

        if table_alias == &alias {
            let values = match values {
                Some(values) => values.iter().map(Rc::clone).collect(),
                None => columns.iter().map(|_| Rc::new(Value::Empty)).collect(),
            };

            Some(values)
        } else {
            next.as_ref().and_then(|next| next.get_alias_values(alias))
        }
    }
}

fn evaluate_blended<T: 'static + Debug>(
    storage: &dyn Store<T>,
    filter_context: Option<&FilterContext<'_>>,
    context: &Context<'_>,
    aggregated: Option<&HashMap<&Function, Value>>,
    expr: &Expr,
) -> Result<Value> {
    let Context {
        table_alias,
        columns,
        values,
        next,
    } = context;

    // TODO: Remove clone
    let row = values.as_ref().map(|values| {
        let values = values.iter().map(|v| Value::clone(&v)).collect();

        Row(values)
    });

    let row_context = row
        .as_ref()
        .map(|row| FilterContext::new(table_alias, &columns, row, filter_context));
    let filter_context = row_context.as_ref().or(filter_context);

    match next {
        Some(context) => evaluate_blended(storage, filter_context, context, aggregated, expr),
        None => match evaluate(storage, filter_context, aggregated, expr)? {
            Evaluated::LiteralRef(v) => Value::try_from(v),
            Evaluated::Literal(v) => Value::try_from(&v),
            Evaluated::StringRef(v) => Ok(Value::Str(v.to_string())),
            Evaluated::ValueRef(v) => Ok(v.clone()),
            Evaluated::Value(v) => Ok(v),
        },
    }
}
