use {
    super::{aggregation_node, filter_node, having_node, join_node, table_factor_node},
    crate::{
        data::{Row, SCHEMALESS_DOC_COLUMN, Value},
        executor::{
            context::{AggregateContext, AggregateValues, RowContext},
            evaluate::evaluate,
            fetch::fetch_project_labels,
        },
        plan::{
            AggregationInputPlan, FilterInputPlan, JoinInputPlan, JoinPlan, ProjectInputPlan,
            ProjectPlan, ProjectionPlan, SelectItemPlan,
        },
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

type ProjectedRow<'a> = (Option<Rc<AggregateValues>>, Option<Rc<RowContext<'a>>>, Row);
type ProjectedIter<'a> = Box<dyn Iterator<Item = Result<ProjectedRow<'a>>> + 'a>;
type ProjectInputIter<'a> = Box<dyn Iterator<Item = Result<AggregateContext<'a>>> + 'a>;

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
    let rows: ProjectInputIter<'a> = match input {
        ProjectInputPlan::Relation(relation) => {
            let rows = table_factor_node::execute(storage, relation)?.map(|context| {
                context.map(|context| AggregateContext {
                    aggregated: None,
                    next: Some(context),
                })
            });

            Box::new(rows)
        }
        ProjectInputPlan::Join(join) => {
            let rows = join_node::execute(storage, join, filter_context.as_ref())?.map(|context| {
                context.map(|context| AggregateContext {
                    aggregated: None,
                    next: Some(context),
                })
            });

            Box::new(rows)
        }
        ProjectInputPlan::Filter(filter) => {
            let rows =
                filter_node::execute(storage, filter, filter_context.as_ref())?.map(|context| {
                    context.map(|context| AggregateContext {
                        aggregated: None,
                        next: Some(context),
                    })
                });

            Box::new(rows)
        }
        ProjectInputPlan::Aggregation(aggregation) => {
            let rows = aggregation_node::execute(storage, aggregation, filter_context.as_ref())?
                .into_iter()
                .map(Ok);

            Box::new(rows)
        }
        ProjectInputPlan::Having(having) => {
            let rows = having_node::execute(storage, having, filter_context.as_ref())?
                .into_iter()
                .map(Ok);

            Box::new(rows)
        }
    };
    let labels = fetch_project_labels(storage, input, projection)?;
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
        table_alias: project_table_alias(input),
    })
}

fn project_table_alias(input: &ProjectInputPlan) -> &str {
    match input {
        ProjectInputPlan::Relation(relation) => relation.alias_name(),
        ProjectInputPlan::Join(join) => join_table_alias(join),
        ProjectInputPlan::Filter(filter) => match &filter.input {
            FilterInputPlan::Relation(relation) => relation.alias_name(),
            FilterInputPlan::Join(join) => join_table_alias(join),
        },
        ProjectInputPlan::Aggregation(aggregation) => match &aggregation.input {
            AggregationInputPlan::Relation(relation) => relation.alias_name(),
            AggregationInputPlan::Join(join) => join_table_alias(join),
            AggregationInputPlan::Filter(filter) => match &filter.input {
                FilterInputPlan::Relation(relation) => relation.alias_name(),
                FilterInputPlan::Join(join) => join_table_alias(join),
            },
        },
        ProjectInputPlan::Having(having) => match &having.input.input {
            AggregationInputPlan::Relation(relation) => relation.alias_name(),
            AggregationInputPlan::Join(join) => join_table_alias(join),
            AggregationInputPlan::Filter(filter) => match &filter.input {
                FilterInputPlan::Relation(relation) => relation.alias_name(),
                FilterInputPlan::Join(join) => join_table_alias(join),
            },
        },
    }
}

fn join_table_alias(join: &JoinPlan) -> &str {
    match &join.input {
        JoinInputPlan::Relation(relation) => relation.alias_name(),
        JoinInputPlan::Join(join) => join_table_alias(join),
    }
}
