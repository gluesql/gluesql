use {
    crate::{
        ast::{Aggregate, Projection, SelectItem},
        data::{Row, SCHEMALESS_DOC_COLUMN, Value},
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
    projection: &'a Projection,
}

impl<'a, T: GStore> Project<'a, T> {
    pub fn new(
        storage: &'a T,
        context: Option<Arc<RowContext<'a>>>,
        projection: &'a Projection,
    ) -> Self {
        Self {
            storage,
            context,
            projection,
        }
    }

    pub async fn apply(
        &self,
        aggregated: Option<Arc<HashMap<&'a Aggregate, Value>>>,
        labels: Arc<[String]>,
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

        match self.projection {
            Projection::SelectItems(fields) => {
                let entries = stream::iter(fields)
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

                let values = entries.into_iter().map(|(_, value)| value).collect();
                let columns = Arc::clone(&labels);

                Ok(Row { columns, values })
            }
            Projection::SchemalessMap => {
                let value = context
                    .get_value(SCHEMALESS_DOC_COLUMN)
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
