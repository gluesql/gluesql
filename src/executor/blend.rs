use {
    super::{
        context::{AggregateContext, BlendContext, FilterContext},
        evaluate::evaluate,
    },
    crate::{
        ast::{Function, SelectItem},
        data::{get_name, Row, Value},
        result::{Error, Result},
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStreamExt},
    im_rc::HashMap,
    serde::Serialize,
    std::convert::TryInto,
    std::{fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum BlendError {
    #[error("table alias not found: {0}")]
    TableAliasNotFound(String),
}

pub struct Blend<'a, T: 'static + Debug> {
    storage: &'a dyn GStore<T>,
    fields: &'a [SelectItem],
}

impl<'a, T: 'static + Debug> Blend<'a, T> {
    pub fn new(storage: &'a dyn GStore<T>, fields: &'a [SelectItem]) -> Self {
        Self { storage, fields }
    }

    pub async fn apply(&self, context: AggregateContext<'a>) -> Result<Row> {
        let AggregateContext { aggregated, next } = context;
        let values = self.blend(aggregated, next).await?;

        Ok(Row(values))
    }

    async fn blend(
        &self,
        aggregated: Option<HashMap<&'a Function, Value>>,
        context: Rc<BlendContext<'a>>,
    ) -> Result<Vec<Value>> {
        let filter_context = FilterContext::concat(None, Some(Rc::clone(&context)));
        let filter_context = Some(filter_context).map(Rc::new);

        let aggregated = aggregated.map(Rc::new);

        let values = stream::iter(self.fields.iter())
            .map(Ok::<&'a SelectItem, Error>)
            .and_then(|item| {
                let context = Rc::clone(&context);
                let filter_context = filter_context.as_ref().map(Rc::clone);
                let aggregated = aggregated.as_ref().map(Rc::clone);

                async move {
                    match item {
                        SelectItem::Wildcard => Ok(context.get_all_values()),
                        SelectItem::QualifiedWildcard(alias) => {
                            let table_alias = get_name(alias)?;

                            match context.get_alias_values(table_alias) {
                                Some(values) => Ok(values),
                                None => {
                                    Err(BlendError::TableAliasNotFound(table_alias.to_string())
                                        .into())
                                }
                            }
                        }
                        SelectItem::Expr { expr, .. } => {
                            evaluate(self.storage, filter_context, aggregated, expr, true)
                                .await
                                .map(|evaluated| evaluated.try_into())?
                                .map(|v| vec![v])
                        }
                    }
                }
            })
            .try_collect::<Vec<Vec<_>>>()
            .await?
            .concat();

        Ok(values)
    }
}
