use {
    crate::{description::TableDescription, error::ResultExt},
    bson::{doc, Document},
    gluesql_core::{
        ast::{ForeignKey, UniqueConstraint},
        error::Result,
    },
    mongodb::options::CreateCollectionOptions,
    serde_json::to_string, std::collections::HashMap,
};

pub struct Validator {
    pub document: Document,
}

impl Validator {
    pub fn new(
        labels: Vec<String>,
        column_types: Document,
        foreign_keys: Vec<ForeignKey>,
        primary_key: Option<Vec<usize>>,
        unique_constraints: Vec<UniqueConstraint>,
        triggers: HashMap<String, gluesql_core::data::Trigger>,
        comment: Option<String>,
    ) -> Result<Self> {
        let mut required = vec![crate::PRIMARY_KEY_SYMBOL.to_owned()];
        required.extend(labels);

        let mut properties = doc! {
            crate::PRIMARY_KEY_SYMBOL: { "bsonType": ["objectId", "binData"] }
        };
        properties.extend(column_types);

        let additional_properties = matches!(required.len(), 1);
        let table_description = to_string(
            &(TableDescription {
                foreign_keys,
                primary_key,
                unique_constraints,
                triggers,
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
