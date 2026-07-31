use {
    super::select_node,
    crate::{
        data::{Row, SCHEMALESS_DOC_COLUMN, Value},
        executor::{
            context::{AggregateContext, AggregateValues, RowContext},
            evaluate::evaluate,
            fetch::fetch_labels,
        },
        plan::{ProjectPlan, ProjectionPlan, SelectItemPlan, TableWithJoinsPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

type ProjectedRow<'a> = (Option<Rc<AggregateValues>>, Option<Rc<RowContext<'a>>>, Row);
type ProjectedIter<'a> = Box<dyn Iterator<Item = Result<ProjectedRow<'a>>> + 'a>;

pub(super) struct ProjectedRows<'a> {
    pub(super) labels: Vec<String>,
    pub(super) rows: ProjectedIter<'a>,
    pub(super) table_alias: &'a str,
}

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a ProjectPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<ProjectedRows<'a>>
where
    T: GStore,
{
    let ProjectPlan { input, projection } = plan;
    let TableWithJoinsPlan { relation, joins } = &input.from;
    let rows = select_node::execute(storage, input, filter_context.as_ref())?;
    let labels = fetch_labels(storage, relation, joins, projection)?;
    let labels = Rc::from(labels);
    let project_labels = Rc::clone(&labels);
    let rows = rows.map(move |aggregate_context| {
        let AggregateContext { aggregated, next } = aggregate_context?;
        let context = match (&next, &filter_context) {
            (Some(context), Some(filter_context)) => Some(Rc::new(RowContext::concat(
                Rc::clone(context),
                Rc::clone(filter_context),
            ))),
            (Some(context), None) => Some(Rc::clone(context)),
            (None, Some(filter_context)) => Some(Rc::clone(filter_context)),
            (None, None) => None,
        };

        let values = match projection {
            ProjectionPlan::SelectItems(fields) => {
                let mut entries = Vec::new();
                for item in fields {
                    match item {
                        SelectItemPlan::Wildcard => {
                            entries.extend(
                                next.as_ref()
                                    .map_or_else(Vec::new, |context| context.get_all_entries()),
                            );
                        }
                        SelectItemPlan::QualifiedWildcard(table_alias) => {
                            entries.extend(
                                next.as_ref()
                                    .and_then(|context| context.get_alias_entries(table_alias))
                                    .unwrap_or_default(),
                            );
                        }
                        SelectItemPlan::Expr { expr, label } => {
                            let value: Value =
                                evaluate(storage, context.as_ref(), aggregated.as_ref(), expr)?
                                    .try_into()?;

                            entries.push((label, value));
                        }
                    }
                }

                entries.into_iter().map(|(_, value)| value).collect()
            }
            ProjectionPlan::SchemalessMap => {
                let value = next
                    .as_ref()
                    .and_then(|context| context.get_value(SCHEMALESS_DOC_COLUMN))
                    .cloned()
                    .unwrap_or(Value::Null);

                vec![value]
            }
        };
        let row = Row {
            columns: Rc::clone(&project_labels),
            values,
        };

        Ok((aggregated, next, row))
    });
    let labels = labels.iter().cloned().collect();

    Ok(ProjectedRows {
        labels,
        rows: Box::new(rows),
        table_alias: relation.alias_name(),
    })
}
