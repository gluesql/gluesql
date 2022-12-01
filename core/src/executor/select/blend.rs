use {
    crate::{
        ast::{Aggregate, SelectItem},
        data::{Row, Value},
        executor::{
            context::{BlendContext, FilterContext},
            evaluate::evaluate,
        },
        result::{Error, Result},
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStreamExt},
    im_rc::HashMap,
    std::rc::Rc,
};

pub struct Blend<'a> {
    storage: &'a dyn GStore,
    filter_context: Option<Rc<FilterContext<'a>>>,
    fields: &'a [SelectItem],
}

impl<'a> Blend<'a> {
    pub fn new(
        storage: &'a dyn GStore,
        filter_context: Option<Rc<FilterContext<'a>>>,
        fields: &'a [SelectItem],
    ) -> Self {
        Self {
            storage,
            filter_context,
            fields,
        }
    }

    pub async fn apply(
        &self,
        aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
        labels: Rc<[String]>,
        context: Rc<BlendContext<'a>>,
    ) -> Result<Row> {
        let filter_context = FilterContext::concat(
            self.filter_context.as_ref().map(Rc::clone),
            Some(Rc::clone(&context)),
        );
        let filter_context = Some(filter_context).map(Rc::new);

        let values = stream::iter(self.fields.iter())
            .map(Ok::<&'a SelectItem, Error>)
            .and_then(|item| {
                let context = Rc::clone(&context);
                let filter_context = filter_context.as_ref().map(Rc::clone);
                let aggregated = aggregated.as_ref().map(Rc::clone);

                async move {
                    match item {
                        SelectItem::Wildcard => Ok(context.get_all_values()),
                        SelectItem::QualifiedWildcard(table_alias) => {
                            Ok(context.get_alias_values(table_alias).unwrap_or_default())
                        }
                        SelectItem::Expr { expr, .. } => {
                            evaluate(self.storage, filter_context, aggregated, expr)
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

        Ok(Row {
            columns: Rc::clone(&labels),
            values,
        })
    }
}
