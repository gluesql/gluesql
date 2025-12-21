use {
    crate::{
        ast::{Aggregate, SelectItem},
        data::{Row, Value},
        executor::{context::RowContext, evaluate::evaluate},
        result::Result,
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStreamExt},
    im::HashMap,
    std::sync::Arc,
};

pub struct Project<'a, T: GStore> {
    storage: &'a T,
    context: Option<Arc<RowContext<'a>>>,
    fields: &'a [SelectItem],
}

impl<'a, T: GStore> Project<'a, T> {
    pub fn new(
        storage: &'a T,
        context: Option<Arc<RowContext<'a>>>,
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
        aggregated: Option<Arc<HashMap<&'a Aggregate, Value>>>,
        labels: Option<Arc<[String]>>,
        context: Arc<RowContext<'a>>,
    ) -> Result<Row> {
        let filter_context = match &self.context {
            Some(filter_context) => Arc::new(RowContext::concat(
                Arc::clone(&context),
                Arc::clone(filter_context),
            )),
            None => Arc::clone(&context),
        };
        let filter_context = Some(filter_context);
        let context = &context;

        let entries = stream::iter(self.fields)
            .then(|item| {
                let filter_context = filter_context.as_ref().map(Arc::clone);
                let aggregated = aggregated.as_ref().map(Arc::clone);

                async move {
                    match item {
                        SelectItem::Wildcard => Ok(context.get_all_entries()),
                        SelectItem::QualifiedWildcard(table_alias) => {
                            Ok(context.get_alias_entries(table_alias).unwrap_or_default())
                        }
                        SelectItem::Expr { expr, label } => {
                            evaluate(self.storage, filter_context, aggregated, expr)
                                .await
                                .map(TryInto::try_into)?
                                .map(|v| vec![(label, v)])
                        }
                    }
                }
            })
            .try_collect::<Vec<Vec<(&String, Value)>>>()
            .await?
            .concat();

        let (cols, values): (Vec<&String>, Vec<_>) = entries.into_iter().unzip();
        let columns = labels.map_or_else(
            || cols.into_iter().cloned().collect::<Vec<_>>().into(),
            |l| Arc::clone(&l),
        );

        Ok(Row { columns, values })
    }
}
