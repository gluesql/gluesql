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

/// Returns the primary key of the table.
///
/// # Implementation details
/// When the table has a primary key, it returns the primary key.
/// When the primary key is a composite key, it returns all the columns in the primary key.
/// When the table does not have a primary key, it returns a None.
pub(crate) fn get_primary_key<'a>(column_defs: &'a [ColumnDef]) -> Option<Vec<&'a ColumnDef>> {
    let primary_keys: Vec<&ColumnDef> = column_defs
        .iter()
        .filter(|column_def| column_def.is_primary())
        .collect();

    if primary_keys.is_empty() {
        None
    } else {
        Some(primary_keys)
    }
}

/// Returns the document object for the primary key.
pub(crate) fn get_primary_key_sort_document<'a>(column_defs: &'a [ColumnDef]) -> Option<Document> {
    get_primary_key(column_defs).map(|primary_keys| {
        primary_keys
            .iter()
            .fold(Document::new(), |mut document, column_def| {
                document.insert(column_def.name.clone(), 1);
                document
            })
    })
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
