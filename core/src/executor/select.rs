use {
    super::{
        execute::{ExecuteError, Payload},
        query,
    },
    crate::{
        data::Value,
        plan::{ProjectionPlan, QueryPlan},
        result::Result,
        store::GStore,
    },
};

#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(
        name = "gluesql.result.materialize",
        target = "gluesql",
        level = "debug",
        on_ok(payload, record(buffered_rows = match payload {
            Payload::Select { rows, .. } => rows.len(),
            Payload::SelectMap(rows) => rows.len(),
            _ => unreachable!("select executor returned a non-select payload"),
        }))
    )
)]
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
    query
        .project()
        .is_some_and(|project| matches!(project.projection, ProjectionPlan::SchemalessMap))
}
