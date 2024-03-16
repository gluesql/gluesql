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

pub struct Validator {
    pub document: Document,
}

impl Validator {
    pub fn new(labels: Vec<String>, column_types: Document) -> Self {
        let mut required = vec!["_id".to_owned()];
        required.extend(labels);

        let mut properties = doc! {
            "_id": { "bsonType": ["objectId", "binData"] }
        };
        properties.extend(column_types);

        let additional_properties = matches!(required.len(), 1);

        let document = doc! {
            "$jsonSchema": {
                "type": "object",
                "required": required,
                "properties": properties,
                "additionalProperties": additional_properties
              }
        };

        Validator { document }
    }

    pub fn to_options(self) -> CreateCollectionOptions {
        CreateCollectionOptions::builder()
            .validator(Some(self.document))
            .build()
    }
}
