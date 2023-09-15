use bson::{doc, Document};
use gluesql_core::ast::ColumnDef;
use mongodb::options::CreateCollectionOptions;

pub fn get_primary_key(column_defs: &[ColumnDef]) -> Option<&ColumnDef> {
    column_defs
        .iter()
        .find(|column_def| column_def.unique.map(|x| x.is_primary).unwrap_or(false))
}

pub fn get_collection_options(
    labels: Vec<String>,
    properties: Document,
    additional_properties: bool,
) -> CreateCollectionOptions {
    CreateCollectionOptions::builder()
        .validator(Some(doc! {
            "$jsonSchema": {
                "type": "object",
                "required": labels,
                "properties": properties,
                "additionalProperties": additional_properties
              }
        }))
        .build()
}
