use {
    bson::{doc, Document},
    gluesql_core::ast::ColumnDef,
    mongodb::options::CreateCollectionOptions,
};

pub fn get_primary_key(column_defs: &[ColumnDef]) -> Option<&ColumnDef> {
    column_defs
        .iter()
        .find(|column_def| column_def.unique.map(|x| x.is_primary).unwrap_or(false))
}

pub fn get_collection_options(
    labels: Vec<String>,
    column_types: Document,
) -> CreateCollectionOptions {
    let mut required = vec!["_id".to_owned()];
    required.extend(labels);

    let mut properties = doc! {
        "_id": { "bsonType": ["objectId", "binData"] }
    };
    properties.extend(column_types);

    let additional_properties = matches!(required.len(), 1);

    CreateCollectionOptions::builder()
        .validator(Some(doc! {
            "$jsonSchema": {
                "type": "object",
                "required": required,
                "properties": properties,
                "additionalProperties": additional_properties
              }
        }))
        .build()
}
