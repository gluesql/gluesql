use im_rc::HashMap;
use iter_enum::Iterator;
use serde::Serialize;
use std::convert::TryInto;
use std::fmt::Debug;
use std::iter::once;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Function, SelectItem};

use super::context::{AggregateContext, BlendContext};
use super::evaluate::evaluate;
use crate::data::{get_name, Row, Value};
use crate::result::Result;
use crate::store::Store;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum BlendError {
    #[error("table alias not found: {0}")]
    TableAliasNotFound(String),
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
        let values = self.blend(aggregated, next)?;

        Ok(Row(values))
    }

    fn blend(
        &self,
        aggregated: Option<HashMap<&'a Function, Value>>,
        context: Rc<BlendContext<'a>>,
    ) -> Result<Vec<Value>> {
        macro_rules! err {
            ($err: expr) => {
                Blended::Err(once(Err($err.into())))
            };
        }

        macro_rules! try_into {
            ($v: expr) => {
                match $v {
                    Ok(v) => v,
                    Err(e) => {
                        return err!(e);
                    }
                }
            };
        }

        let filter_context = context.concat_into(None);

        self.fields
            .iter()
            .flat_map(|item| match item {
                SelectItem::Wildcard => {
                    let values = context.get_all_values().into_iter().map(Ok);

                    Blended::All(values)
                }
                SelectItem::QualifiedWildcard(alias) => {
                    let table_alias = try_into!(get_name(alias));

                    match context.get_alias_values(table_alias) {
                        Some(values) => Blended::AllInTable(values.into_iter().map(Ok)),
                        None => err!(BlendError::TableAliasNotFound(table_alias.to_string())),
                    }
                }
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    let filter_context = filter_context.as_ref().map(Rc::clone);

                    let value = evaluate(self.storage, filter_context, aggregated.as_ref(), expr)
                        .map(|evaluated| evaluated.try_into());
                    let value = try_into!(value);

                    Blended::Single(once(value))
                }
            })
            .collect::<Result<_>>()
    }
}
