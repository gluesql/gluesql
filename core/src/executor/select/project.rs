use {
    crate::{
        ast::{Aggregate, SelectItem},
        data::{Row, Value},
        executor::{context::RowContext, evaluate::evaluate},
        result::{Error, Result},
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStreamExt},
    im_rc::HashMap,
    std::rc::Rc,
};

pub struct Project<'a> {
    storage: &'a dyn GStore,
    context: Option<Rc<RowContext<'a>>>,
    fields: &'a [SelectItem],
}

impl<'a> Project<'a> {
    pub fn new(
        storage: &'a dyn GStore,
        context: Option<Rc<RowContext<'a>>>,
        fields: &'a [SelectItem],
    ) -> Self {
        Self {
            storage,
            context,
            fields,
        }
    }

    pub async fn apply(
        &self,
        aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
        labels: Rc<[String]>,
        context: Rc<RowContext<'a>>,
    ) -> Result<Row> {
        let filter_context = match &self.context {
            Some(filter_context) => Rc::new(RowContext::concat(
                Rc::clone(&context),
                Rc::clone(filter_context),
            )),
            None => Rc::clone(&context),
        };
        let filter_context = Some(filter_context);

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
