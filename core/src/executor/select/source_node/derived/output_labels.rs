use crate::{
    executor::select::{SelectedSources, projection_labels, values_node},
    plan::{
        AggregationInputPlan, DistinctInputPlan, DistinctPlan, FilterInputPlan, FilterPlan,
        JoinInputPlan, JoinPlan, LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan,
        ProjectInputPlan, ProjectPlan, QueryPlan,
    },
    result::Result,
    store::GStore,
};

pub(super) fn query<'a, T: GStore>(storage: &'a T, query: &'a QueryPlan) -> Result<Vec<String>> {
    match query {
        QueryPlan::Project(project) => project_plan(storage, project),
        QueryPlan::Values(values) => Ok(values_node::labels(values)),
        QueryPlan::SelectOrderBy(order_by) => project_plan(storage, &order_by.input),
        QueryPlan::ValuesOrderBy(order_by) => Ok(values_node::labels(&order_by.input)),
        QueryPlan::Distinct(plan) => distinct(storage, plan),
        QueryPlan::Offset(plan) => offset(storage, plan),
        QueryPlan::Limit(plan) => limit(storage, plan),
    }
}

fn project_plan<'a, T: GStore>(storage: &'a T, plan: &'a ProjectPlan) -> Result<Vec<String>> {
    let sources = project_sources(storage, &plan.input)?;

    projection_labels::resolve(&sources, &plan.projection)
}

fn distinct<'a, T: GStore>(storage: &'a T, plan: &'a DistinctPlan) -> Result<Vec<String>> {
    match &plan.input {
        DistinctInputPlan::Project(project) => project_plan(storage, project),
        DistinctInputPlan::SelectOrderBy(order_by) => project_plan(storage, &order_by.input),
    }
}

fn offset<'a, T: GStore>(storage: &'a T, plan: &'a OffsetPlan) -> Result<Vec<String>> {
    match &plan.input {
        OffsetInputPlan::Project(project) => project_plan(storage, project),
        OffsetInputPlan::Values(values) => Ok(values_node::labels(values)),
        OffsetInputPlan::SelectOrderBy(order_by) => project_plan(storage, &order_by.input),
        OffsetInputPlan::ValuesOrderBy(order_by) => Ok(values_node::labels(&order_by.input)),
        OffsetInputPlan::Distinct(distinct_plan) => distinct(storage, distinct_plan),
    }
}

fn limit<'a, T: GStore>(storage: &'a T, plan: &'a LimitPlan) -> Result<Vec<String>> {
    match &plan.input {
        LimitInputPlan::Project(project) => project_plan(storage, project),
        LimitInputPlan::Values(values) => Ok(values_node::labels(values)),
        LimitInputPlan::SelectOrderBy(order_by) => project_plan(storage, &order_by.input),
        LimitInputPlan::ValuesOrderBy(order_by) => Ok(values_node::labels(&order_by.input)),
        LimitInputPlan::Distinct(distinct_plan) => distinct(storage, distinct_plan),
        LimitInputPlan::Offset(offset_plan) => offset(storage, offset_plan),
    }
}

fn project_sources<'a, T: GStore>(
    storage: &'a T,
    input: &'a ProjectInputPlan,
) -> Result<SelectedSources<'a>> {
    match input {
        ProjectInputPlan::Source(source) => source_columns(storage, source),
        ProjectInputPlan::Join(join) => join_sources(storage, join),
        ProjectInputPlan::Filter(filter) => filter_sources(storage, filter),
        ProjectInputPlan::Aggregation(aggregation) => {
            aggregation_sources(storage, &aggregation.input)
        }
        ProjectInputPlan::Having(having) => aggregation_sources(storage, &having.input.input),
    }
}

fn aggregation_sources<'a, T: GStore>(
    storage: &'a T,
    input: &'a AggregationInputPlan,
) -> Result<SelectedSources<'a>> {
    match input {
        AggregationInputPlan::Source(source) => source_columns(storage, source),
        AggregationInputPlan::Join(join) => join_sources(storage, join),
        AggregationInputPlan::Filter(filter) => filter_sources(storage, filter),
    }
}

fn filter_sources<'a, T: GStore>(
    storage: &'a T,
    filter: &'a FilterPlan,
) -> Result<SelectedSources<'a>> {
    match &filter.input {
        FilterInputPlan::Source(source) => source_columns(storage, source),
        FilterInputPlan::Join(join) => join_sources(storage, join),
    }
}

fn join_sources<'a, T: GStore>(storage: &'a T, join: &'a JoinPlan) -> Result<SelectedSources<'a>> {
    let mut sources = match &join.input {
        JoinInputPlan::Source(source) => source_columns(storage, source)?,
        JoinInputPlan::Join(join) => join_sources(storage, join)?,
    };
    let right = super::super::execute(storage, &join.right)?;
    sources.joined.push(right.output);

    Ok(sources)
}

fn source_columns<'a, T: GStore>(
    storage: &'a T,
    source: &'a crate::plan::SourcePlan,
) -> Result<SelectedSources<'a>> {
    let source = super::super::execute(storage, source)?.output;

    Ok(SelectedSources {
        base: source,
        joined: Vec::new(),
    })
}
