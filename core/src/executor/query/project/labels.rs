use {
    super::super::{QueryError, SelectedSources},
    crate::{
        data::SCHEMALESS_DOC_COLUMN,
        plan::{ProjectionPlan, SelectItemPlan},
        result::Result,
    },
};

pub(super) fn resolve(
    sources: &SelectedSources<'_>,
    projection: &ProjectionPlan,
) -> Result<Vec<String>> {
    match projection {
        ProjectionPlan::SchemalessMap => Ok(vec![SCHEMALESS_DOC_COLUMN.to_owned()]),
        ProjectionPlan::SelectItems(items) => items
            .iter()
            .flat_map(|item| match item {
                SelectItemPlan::Wildcard => sources
                    .base
                    .names
                    .iter()
                    .cloned()
                    .chain(
                        sources
                            .joined
                            .iter()
                            .flat_map(|source| source.names.iter().cloned()),
                    )
                    .map(Ok)
                    .collect(),
                SelectItemPlan::QualifiedWildcard(target) if target == sources.base.alias => {
                    sources.base.names.iter().cloned().map(Ok).collect()
                }
                SelectItemPlan::QualifiedWildcard(target) => sources
                    .joined
                    .iter()
                    .find(|source| source.alias == target)
                    .map_or_else(
                        || vec![Err(QueryError::TableAliasNotFound(target.to_owned()).into())],
                        |source| source.names.iter().cloned().map(Ok).collect(),
                    ),
                SelectItemPlan::Expr { label, .. } => vec![Ok(label.clone())],
            })
            .collect(),
    }
}
