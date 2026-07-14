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
    std::rc::Rc,
};

pub struct Project<'a, T: GStore> {
    storage: &'a T,
    context: Option<Rc<RowContext<'a>>>,
    projection: &'a ProjectionPlan,
}

impl<'a, T: GStore> Project<'a, T> {
    pub fn new(
        storage: &'a T,
        context: Option<Rc<RowContext<'a>>>,
        projection: &'a ProjectionPlan,
    ) -> Self {
        Self {
            storage,
            context,
            projection,
        }
    }

    pub fn apply(
        &self,
        aggregated: Option<&Rc<AggregateValues>>,
        labels: &Rc<[String]>,
        context: Option<&Rc<RowContext<'a>>>,
    ) -> Result<Row> {
        let filter_context = match (&context, &self.context) {
            (Some(context), Some(filter_context)) => Some(Rc::new(RowContext::concat(
                Rc::clone(context),
                Rc::clone(filter_context),
            ))),
            (Some(context), None) => Some(Rc::clone(context)),
            (None, Some(filter_context)) => Some(Rc::clone(filter_context)),
            (None, None) => None,
        };

        match self.projection {
            ProjectionPlan::SelectItems(fields) => {
                let mut entries = Vec::new();
                for item in fields {
                    match item {
                        SelectItemPlan::Wildcard => {
                            entries.extend(
                                context.map_or_else(Vec::new, |context| context.get_all_entries()),
                            );
                        }
                        SelectItemPlan::QualifiedWildcard(table_alias) => {
                            entries.extend(
                                context
                                    .and_then(|context| context.get_alias_entries(table_alias))
                                    .unwrap_or_default(),
                            );
                        }
                        SelectItemPlan::Expr { expr, label } => {
                            let value: Value =
                                evaluate(self.storage, filter_context.as_ref(), aggregated, expr)?
                                    .try_into()?;

                            entries.push((label, value));
                        }
                    }
                }

                let values = entries.into_iter().map(|(_, value)| value).collect();
                let columns = Rc::clone(labels);

                Ok(Row { columns, values })
            }
            ProjectionPlan::SchemalessMap => {
                let value = context
                    .and_then(|context| context.get_value(SCHEMALESS_DOC_COLUMN))
                    .cloned()
                    .unwrap_or(Value::Null);

                Ok(Row {
                    columns: Rc::clone(labels),
                    values: vec![value],
                })
            }
        }
    }
}
