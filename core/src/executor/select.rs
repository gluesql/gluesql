use {
    super::{
        execute::{ExecuteError, Payload},
        query,
    },
    crate::{
        data::Value,
        plan::{
            DistinctInputPlan, DistinctPlan, LimitInputPlan, LimitPlan, OffsetInputPlan,
            OffsetPlan, ProjectPlan, ProjectionPlan, QueryPlan,
        },
        result::Result,
        store::GStore,
    },
};

pub(super) fn execute<T: GStore>(storage: &T, query: &QueryPlan) -> Result<Payload> {
    let (labels, rows) = query::execute_with_labels(storage, query, None)?;

    if is_schemaless_map(query) {
        rows.map(|row| {
            let mut values = row?.into_values().into_iter();
            match (values.next(), values.next()) {
                (Some(Value::Map(map)), None) => Ok(map),
                _ => Err(ExecuteError::ExpectedMapValueInDocColumn.into()),
            }
        })
        .collect::<Result<Vec<_>>>()
        .map(Payload::SelectMap)
    } else {
        rows.map(|row| Ok(row?.into_values()))
            .collect::<Result<Vec<_>>>()
            .map(|rows| Payload::Select { labels, rows })
    }
}

fn is_schemaless_map(query: &QueryPlan) -> bool {
    match query {
        QueryPlan::Project(project) => project_is_schemaless(project),
        QueryPlan::Values(_) | QueryPlan::ValuesOrderBy(_) => false,
        QueryPlan::SelectOrderBy(order_by) => project_is_schemaless(&order_by.input),
        QueryPlan::Distinct(distinct) => distinct_is_schemaless(distinct),
        QueryPlan::Offset(offset) => offset_is_schemaless(offset),
        QueryPlan::Limit(LimitPlan { input, .. }) => match input {
            LimitInputPlan::Project(project) => project_is_schemaless(project),
            LimitInputPlan::Values(_) | LimitInputPlan::ValuesOrderBy(_) => false,
            LimitInputPlan::SelectOrderBy(order_by) => project_is_schemaless(&order_by.input),
            LimitInputPlan::Distinct(distinct) => distinct_is_schemaless(distinct),
            LimitInputPlan::Offset(offset) => offset_is_schemaless(offset),
        },
    }
}

fn offset_is_schemaless(offset: &OffsetPlan) -> bool {
    match &offset.input {
        OffsetInputPlan::Project(project) => project_is_schemaless(project),
        OffsetInputPlan::Values(_) | OffsetInputPlan::ValuesOrderBy(_) => false,
        OffsetInputPlan::SelectOrderBy(order_by) => project_is_schemaless(&order_by.input),
        OffsetInputPlan::Distinct(distinct) => distinct_is_schemaless(distinct),
    }
}

fn distinct_is_schemaless(distinct: &DistinctPlan) -> bool {
    match &distinct.input {
        DistinctInputPlan::Project(project) => project_is_schemaless(project),
        DistinctInputPlan::SelectOrderBy(order_by) => project_is_schemaless(&order_by.input),
    }
}

fn project_is_schemaless(project: &ProjectPlan) -> bool {
    matches!(project.projection, ProjectionPlan::SchemalessMap)
}
