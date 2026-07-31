use {
    super::{aggregation_node, filter_node, having_node, join_node, source_node},
    crate::{
        data::{Row, SCHEMALESS_DOC_COLUMN, Value},
        executor::{
            context::{AggregateContext, AggregateValues, RowContext},
            evaluate::evaluate,
            fetch::FetchError,
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
        ProjectInputPlan::Source(source) => {
            let rows = source_node::execute(storage, source, None)?
                .into_selected(None)
                .map(|context| {
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
    let labels = labels(storage, plan)?;
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

pub(super) fn labels<T: GStore>(storage: &T, plan: &ProjectPlan) -> Result<Vec<String>> {
    let ProjectPlan { input, projection } = plan;

    match input {
        ProjectInputPlan::Source(source) => {
            let columns = source_node::columns(storage, source)?;
            projection_labels(source.alias_name(), &columns, &[], projection)
        }
        ProjectInputPlan::Join(join) => {
            let (alias, columns, joined) = join_node::columns(storage, join)?;
            projection_labels(alias, &columns, &joined, projection)
        }
        ProjectInputPlan::Filter(filter) => match &filter.input {
            FilterInputPlan::Source(source) => {
                let columns = source_node::columns(storage, source)?;
                projection_labels(source.alias_name(), &columns, &[], projection)
            }
            FilterInputPlan::Join(join) => {
                let (alias, columns, joined) = join_node::columns(storage, join)?;
                projection_labels(alias, &columns, &joined, projection)
            }
        },
        ProjectInputPlan::Aggregation(aggregation) => {
            aggregation_labels(storage, &aggregation.input, projection)
        }
        ProjectInputPlan::Having(having) => {
            aggregation_labels(storage, &having.input.input, projection)
        }
    }
}

fn aggregation_labels<T: GStore>(
    storage: &T,
    input: &AggregationInputPlan,
    projection: &ProjectionPlan,
) -> Result<Vec<String>> {
    match input {
        AggregationInputPlan::Source(source) => {
            let columns = source_node::columns(storage, source)?;
            projection_labels(source.alias_name(), &columns, &[], projection)
        }
        AggregationInputPlan::Join(join) => {
            let (alias, columns, joined) = join_node::columns(storage, join)?;
            projection_labels(alias, &columns, &joined, projection)
        }
        AggregationInputPlan::Filter(filter) => match &filter.input {
            FilterInputPlan::Source(source) => {
                let columns = source_node::columns(storage, source)?;
                projection_labels(source.alias_name(), &columns, &[], projection)
            }
            FilterInputPlan::Join(join) => {
                let (alias, columns, joined) = join_node::columns(storage, join)?;
                projection_labels(alias, &columns, &joined, projection)
            }
        },
    }
}

fn projection_labels(
    source_alias: &str,
    source_columns: &[String],
    joined: &[(&str, Rc<[String]>)],
    projection: &ProjectionPlan,
) -> Result<Vec<String>> {
    match projection {
        ProjectionPlan::SchemalessMap => Ok(vec![SCHEMALESS_DOC_COLUMN.to_owned()]),
        ProjectionPlan::SelectItems(items) => items
            .iter()
            .flat_map(|item| match item {
                SelectItemPlan::Wildcard => source_columns
                    .iter()
                    .cloned()
                    .chain(
                        joined
                            .iter()
                            .flat_map(|(_, columns)| columns.iter().cloned()),
                    )
                    .map(Ok)
                    .collect(),
                SelectItemPlan::QualifiedWildcard(target) if target == source_alias => {
                    source_columns.iter().cloned().map(Ok).collect()
                }
                SelectItemPlan::QualifiedWildcard(target) => joined
                    .iter()
                    .find(|(alias, _)| alias == target)
                    .map_or_else(
                        || vec![Err(FetchError::TableAliasNotFound(target.to_owned()).into())],
                        |(_, columns)| columns.iter().cloned().map(Ok).collect(),
                    ),
                SelectItemPlan::Expr { label, .. } => vec![Ok(label.clone())],
            })
            .collect(),
    }
}

fn project_table_alias(input: &ProjectInputPlan) -> &str {
    match input {
        ProjectInputPlan::Source(relation) => relation.alias_name(),
        ProjectInputPlan::Join(join) => join_table_alias(join),
        ProjectInputPlan::Filter(filter) => match &filter.input {
            FilterInputPlan::Source(relation) => relation.alias_name(),
            FilterInputPlan::Join(join) => join_table_alias(join),
        },
        ProjectInputPlan::Aggregation(aggregation) => match &aggregation.input {
            AggregationInputPlan::Source(relation) => relation.alias_name(),
            AggregationInputPlan::Join(join) => join_table_alias(join),
            AggregationInputPlan::Filter(filter) => match &filter.input {
                FilterInputPlan::Source(relation) => relation.alias_name(),
                FilterInputPlan::Join(join) => join_table_alias(join),
            },
        },
        ProjectInputPlan::Having(having) => match &having.input.input {
            AggregationInputPlan::Source(relation) => relation.alias_name(),
            AggregationInputPlan::Join(join) => join_table_alias(join),
            AggregationInputPlan::Filter(filter) => match &filter.input {
                FilterInputPlan::Source(relation) => relation.alias_name(),
                FilterInputPlan::Join(join) => join_table_alias(join),
            },
        },
    }
}

fn join_table_alias(join: &JoinPlan) -> &str {
    match &join.input {
        JoinInputPlan::Source(relation) => relation.alias_name(),
        JoinInputPlan::Join(join) => join_table_alias(join),
    }
}
