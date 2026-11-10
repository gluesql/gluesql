use crate::{
    executor::{
        context::StatementContext,
        query::{OutputBody, SelectedSources, output_body, project, source, values},
    },
    plan::{ProjectPlan, QueryPlan},
    result::Result,
    store::GStore,
};

pub(super) fn query<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    query: &'a QueryPlan,
) -> Result<Vec<String>> {
    match output_body(query) {
        OutputBody::Project(project) => project_plan(storage, stmt_context, project),
        OutputBody::Values(values_plan) => Ok(values::labels(values_plan)),
    }
}

fn project_plan<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    plan: &'a ProjectPlan,
) -> Result<Vec<String>> {
    let base = source::execute(storage, stmt_context, plan.input.base_source())?.output;
    let joined = plan
        .input
        .joined_sources()
        .into_iter()
        .map(|source| source::execute(storage, stmt_context, source).map(|source| source.output))
        .collect::<Result<Vec<_>>>()?;
    let sources = SelectedSources { base, joined };

    project::resolve_labels(&sources, &plan.projection)
}
