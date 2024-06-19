use {
    crate::{description::TableDescription, error::ResultExt},
    bson::{doc, Document},
    gluesql_core::{
        ast::{ColumnDef, ForeignKey},
        error::Result,
    },
    mongodb::options::CreateCollectionOptions,
    serde_json::to_string,
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
    pub fn new(
        labels: Vec<String>,
        column_types: Document,
        foreign_keys: Vec<ForeignKey>,
        comment: Option<String>,
    ) -> Result<Self> {
        let mut required = vec!["_id".to_owned()];
        required.extend(labels);

        let mut properties = doc! {
            "_id": { "bsonType": ["objectId", "binData"] }
        };
        properties.extend(column_types);

        let additional_properties = matches!(required.len(), 1);
        let table_description = to_string(
            &(TableDescription {
                foreign_keys,
                comment,
            }),
        )
        .map_storage_err()?;

        let document = doc! {
            "$jsonSchema": {
                "type": "object",
                "required": required,
                "properties": properties,
                "description": table_description,
                "additionalProperties": additional_properties
              }
        };

        Ok(Self { document })
    }

    pub fn to_options(self) -> CreateCollectionOptions {
        CreateCollectionOptions::builder()
            .validator(Some(self.document))
            .build()
    }
}
