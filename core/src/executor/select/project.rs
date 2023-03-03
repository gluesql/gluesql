use {
    crate::{
        ast::{Aggregate, SelectItem},
        data::{Row, Value},
        executor::{context::RowContext, evaluate::evaluate},
        result::Result,
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStreamExt},
    im_rc::HashMap,
    std::rc::Rc,
};

pub struct Project<'a, T: GStore> {
    storage: &'a T,
    context: Option<Rc<RowContext<'a>>>,
    fields: &'a [SelectItem],
}

impl<'a, T: GStore> Project<'a, T> {
    pub fn new(
        storage: &'a T,
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
        labels: Option<Rc<[String]>>,
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
        let context = &context;

        let entries = stream::iter(self.fields)
            .then(|item| {
                let filter_context = filter_context.as_ref().map(Rc::clone);
                let aggregated = aggregated.as_ref().map(Rc::clone);

                async move {
                    match item {
                        SelectItem::Wildcard => Ok(context.get_all_entries()),
                        SelectItem::QualifiedWildcard(table_alias) => {
                            Ok(context.get_alias_entries(table_alias).unwrap_or_default())
                        }
                        SelectItem::Expr { expr, label } => {
                            evaluate(self.storage, filter_context, aggregated, expr)
                                .await
                                .map(|evaluated| evaluated.try_into())?
                                .map(|v| vec![(label, v)])
                        }
                    }
                }
            })
            .try_collect::<Vec<Vec<(&String, Value)>>>()
            .await?
            .concat();

        Ok(match labels {
            Some(labels) => Row::Vec {
                columns: Rc::clone(&labels),
                values: entries.into_iter().map(|(_, v)| v).collect(),
            },
            None => Row::Map(entries.into_iter().map(|(k, v)| (k.clone(), v)).collect()),
        })
    }
}
