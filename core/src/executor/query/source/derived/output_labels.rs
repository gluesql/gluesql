use crate::{
    executor::query::{SelectedSources, project, source, values},
    plan::{
        DistinctInputPlan, DistinctPlan, LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan,
        ProjectPlan, QueryPlan,
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
    let base = source::execute(storage, plan.input.base_source())?.output;
    let joined = plan
        .input
        .joined_sources()
        .into_iter()
        .map(|source| source::execute(storage, source).map(|source| source.output))
        .collect::<Result<Vec<_>>>()?;
    let sources = SelectedSources { base, joined };

    project::resolve_labels(&sources, &plan.projection)
}
