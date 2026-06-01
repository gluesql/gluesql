use {
    crate::{
        data::{Row, SCHEMALESS_DOC_COLUMN, Value},
        executor::{
            context::{AggregateValues, RowContext},
            evaluate::evaluate,
        },
        plan::{ProjectionPlan, SelectItemPlan},
        result::Result,
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStreamExt},
    std::sync::Arc,
};

pub struct Project<'a, T: GStore> {
    storage: &'a T,
    context: Option<Arc<RowContext<'a>>>,
    projection: &'a ProjectionPlan,
}

impl<'a, T: GStore> Project<'a, T> {
    pub fn new(
        storage: &'a T,
        context: Option<Arc<RowContext<'a>>>,
        projection: &'a ProjectionPlan,
    ) -> Self {
        Self {
            storage,
            context,
            projection,
        }
    }

    pub async fn apply(
        &self,
        aggregated: Option<Arc<AggregateValues>>,
        labels: Arc<[String]>,
        context: Option<Arc<RowContext<'a>>>,
    ) -> Result<Row> {
        let filter_context = match (&context, &self.context) {
            (Some(context), Some(filter_context)) => Some(Arc::new(RowContext::concat(
                Arc::clone(context),
                Arc::clone(filter_context),
            ))),
            (Some(context), None) => Some(Arc::clone(context)),
            (None, Some(filter_context)) => Some(Arc::clone(filter_context)),
            (None, None) => None,
        };
        let context = context.as_ref();

        match self.projection {
            ProjectionPlan::SelectItems(fields) => {
                let entries = stream::iter(fields)
                    .then(|item| {
                        let filter_context = filter_context.as_ref().map(Arc::clone);
                        let aggregated = aggregated.as_ref().map(Arc::clone);

                        async move {
                            match item {
                                SelectItemPlan::Wildcard => Ok(context
                                    .map_or_else(Vec::new, |context| context.get_all_entries())),
                                SelectItemPlan::QualifiedWildcard(table_alias) => Ok(context
                                    .and_then(|context| context.get_alias_entries(table_alias))
                                    .unwrap_or_default()),
                                SelectItemPlan::Expr { expr, label } => {
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

                let values = entries.into_iter().map(|(_, value)| value).collect();
                let columns = Arc::clone(&labels);

                Ok(Row { columns, values })
            }
            ProjectionPlan::SchemalessMap => {
                let value = context
                    .and_then(|context| context.get_value(SCHEMALESS_DOC_COLUMN))
                    .cloned()
                    .unwrap_or(Value::Null);

                Ok(Row {
                    columns: Arc::clone(&labels),
                    values: vec![value],
                })
            }
        }
    }
}
