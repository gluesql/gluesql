use {
    crate::{
        ast::{Aggregate, SelectItem},
        data::{Row, Value},
        executor::{context::RowContext, evaluate::evaluate},
        result::Result,
        store::GStore,
        Grc, HashMap,
    },
    futures::stream::{self, StreamExt, TryStreamExt},
};

pub struct Project<'a, T> {
    storage: &'a T,
    context: Option<Grc<RowContext<'a>>>,
    fields: &'a [SelectItem],
}

impl<
        'a,
        #[cfg(feature = "send")] T: GStore + Send + Sync,
        #[cfg(not(feature = "send"))] T: GStore,
    > Project<'a, T>
{
    pub fn new(
        storage: &'a T,
        context: Option<Grc<RowContext<'a>>>,
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
        aggregated: Option<Grc<HashMap<&'a Aggregate, Value>>>,
        labels: Option<Grc<[String]>>,
        context: Grc<RowContext<'a>>,
    ) -> Result<Row> {
        let filter_context = match &self.context {
            Some(filter_context) => Grc::new(RowContext::concat(
                Grc::clone(&context),
                Grc::clone(filter_context),
            )),
            None => Grc::clone(&context),
        };
        let filter_context = Some(filter_context);
        let context = &context;

        let entries = stream::iter(self.fields)
            .then(|item| {
                let filter_context = filter_context.as_ref().map(Grc::clone);
                let aggregated = aggregated.as_ref().map(Grc::clone);

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
                columns: Grc::clone(&labels),
                values: entries.into_iter().map(|(_, v)| v).collect(),
            },
            None => Row::Map(entries.into_iter().map(|(k, v)| (k.clone(), v)).collect()),
        })
    }
}
