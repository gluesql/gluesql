use crate::{
    executor::query::{SelectedSources, project, source, values},
    plan::{
        AggregationInputPlan, DistinctInputPlan, DistinctPlan, FilterInputPlan, FilterPlan,
        HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan,
        JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan, LimitInputPlan, LimitPlan,
        NestedLoopJoinInputPlan, NestedLoopJoinPlan, OffsetInputPlan, OffsetPlan, ProjectInputPlan,
        ProjectPlan, QueryPlan, SourcePlan,
    },
    result::Result,
    store::GStore,
};

pub(super) fn query<'a, T: GStore>(storage: &'a T, query: &'a QueryPlan) -> Result<Vec<String>> {
    match query {
        QueryPlan::Project(project) => project_plan(storage, project),
        QueryPlan::Values(values_plan) => Ok(values::labels(values_plan)),
        QueryPlan::SelectOrderBy(order_by) => project_plan(storage, &order_by.input),
        QueryPlan::ValuesOrderBy(order_by) => Ok(values::labels(&order_by.input)),
        QueryPlan::Distinct(plan) => distinct(storage, plan),
        QueryPlan::Offset(plan) => offset(storage, plan),
        QueryPlan::Limit(plan) => limit(storage, plan),
    }
}

fn limit<'a, T: GStore>(storage: &'a T, plan: &'a LimitPlan) -> Result<Vec<String>> {
    match &plan.input {
        LimitInputPlan::Project(project) => project_plan(storage, project),
        LimitInputPlan::Values(values_plan) => Ok(values::labels(values_plan)),
        LimitInputPlan::SelectOrderBy(order_by) => project_plan(storage, &order_by.input),
        LimitInputPlan::ValuesOrderBy(order_by) => Ok(values::labels(&order_by.input)),
        LimitInputPlan::Distinct(distinct_plan) => distinct(storage, distinct_plan),
        LimitInputPlan::Offset(offset_plan) => offset(storage, offset_plan),
    }
}

fn offset<'a, T: GStore>(storage: &'a T, plan: &'a OffsetPlan) -> Result<Vec<String>> {
    match &plan.input {
        OffsetInputPlan::Project(project) => project_plan(storage, project),
        OffsetInputPlan::Values(values_plan) => Ok(values::labels(values_plan)),
        OffsetInputPlan::SelectOrderBy(order_by) => project_plan(storage, &order_by.input),
        OffsetInputPlan::ValuesOrderBy(order_by) => Ok(values::labels(&order_by.input)),
        OffsetInputPlan::Distinct(distinct_plan) => distinct(storage, distinct_plan),
    }
}

fn distinct<'a, T: GStore>(storage: &'a T, plan: &'a DistinctPlan) -> Result<Vec<String>> {
    match &plan.input {
        DistinctInputPlan::Project(project) => project_plan(storage, project),
        DistinctInputPlan::SelectOrderBy(order_by) => project_plan(storage, &order_by.input),
    }
}

fn project_plan<'a, T: GStore>(storage: &'a T, plan: &'a ProjectPlan) -> Result<Vec<String>> {
    let sources = project_sources(storage, &plan.input)?;

    project::resolve_labels(&sources, &plan.projection)
}

fn project_sources<'a, T: GStore>(
    storage: &'a T,
    input: &'a ProjectInputPlan,
) -> Result<SelectedSources<'a>> {
    match input {
        ProjectInputPlan::Source(source) => source_columns(storage, source),
        ProjectInputPlan::InnerJoin(join) => inner_join_sources(storage, join),
        ProjectInputPlan::LeftOuterJoin(join) => left_outer_join_sources(storage, join),
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
        AggregationInputPlan::InnerJoin(join) => inner_join_sources(storage, join),
        AggregationInputPlan::LeftOuterJoin(join) => left_outer_join_sources(storage, join),
        AggregationInputPlan::Filter(filter) => filter_sources(storage, filter),
    }
}

fn filter_sources<'a, T: GStore>(
    storage: &'a T,
    filter: &'a FilterPlan,
) -> Result<SelectedSources<'a>> {
    match &filter.input {
        FilterInputPlan::Source(source) => source_columns(storage, source),
        FilterInputPlan::InnerJoin(join) => inner_join_sources(storage, join),
        FilterInputPlan::LeftOuterJoin(join) => left_outer_join_sources(storage, join),
    }
}

fn inner_join_sources<'a, T: GStore>(
    storage: &'a T,
    join: &'a InnerJoinPlan,
) -> Result<SelectedSources<'a>> {
    match &join.input {
        InnerJoinInputPlan::NestedLoop(join) => nested_loop_sources(storage, join),
        InnerJoinInputPlan::Hash(join) => hash_sources(storage, join),
        InnerJoinInputPlan::Condition(condition) => condition_sources(storage, condition),
    }
}

fn left_outer_join_sources<'a, T: GStore>(
    storage: &'a T,
    join: &'a LeftOuterJoinPlan,
) -> Result<SelectedSources<'a>> {
    match &join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => nested_loop_sources(storage, join),
        LeftOuterJoinInputPlan::Hash(join) => hash_sources(storage, join),
        LeftOuterJoinInputPlan::Condition(condition) => condition_sources(storage, condition),
    }
}

fn condition_sources<'a, T: GStore>(
    storage: &'a T,
    condition: &'a JoinConditionPlan,
) -> Result<SelectedSources<'a>> {
    match &condition.input {
        JoinConditionInputPlan::NestedLoop(join) => nested_loop_sources(storage, join),
        JoinConditionInputPlan::Hash(join) => hash_sources(storage, join),
    }
}

fn nested_loop_sources<'a, T: GStore>(
    storage: &'a T,
    join: &'a NestedLoopJoinPlan,
) -> Result<SelectedSources<'a>> {
    let mut sources = match &join.input {
        NestedLoopJoinInputPlan::Source(source) => source_columns(storage, source)?,
        NestedLoopJoinInputPlan::InnerJoin(join) => inner_join_sources(storage, join)?,
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => left_outer_join_sources(storage, join)?,
    };
    let right = source::execute(storage, &join.right)?;
    sources.joined.push(right.output);

    Ok(sources)
}

fn hash_sources<'a, T: GStore>(
    storage: &'a T,
    join: &'a HashJoinPlan,
) -> Result<SelectedSources<'a>> {
    let mut sources = match &join.input {
        HashJoinInputPlan::Source(source) => source_columns(storage, source)?,
        HashJoinInputPlan::InnerJoin(join) => inner_join_sources(storage, join)?,
        HashJoinInputPlan::LeftOuterJoin(join) => left_outer_join_sources(storage, join)?,
    };
    let right = source::execute(storage, &join.right)?;
    sources.joined.push(right.output);

    Ok(sources)
}

fn source_columns<'a, T: GStore>(
    storage: &'a T,
    source: &'a SourcePlan,
) -> Result<SelectedSources<'a>> {
    let source = source::execute(storage, source)?.output;

    Ok(SelectedSources {
        base: source,
        joined: Vec::new(),
    })
}
