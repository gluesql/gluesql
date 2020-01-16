use nom_sql::{Column, ColumnSpecification, Literal};
use serde::{Deserialize, Serialize};
use std::convert::From;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Row {
    pub key: String,
    items: Vec<(Column, Literal)>,
}

impl Row {
    pub fn get_literal(&self, column_name: &str) -> Option<&Literal> {
        self.items
            .iter()
            .filter(|(column, _)| column.name == column_name)
            .map(|(_, literal)| literal)
            .nth(0)
    }
}

impl
    From<(
        Vec<ColumnSpecification>,
        Option<Vec<Column>>,
        Vec<Vec<Literal>>,
    )> for Row
{
    fn from(
        (create_fields, insert_fields, insert_data): (
            Vec<ColumnSpecification>,
            Option<Vec<Column>>,
            Vec<Vec<Literal>>,
        ),
    ) -> Self {
        let create_fields = create_fields
            .into_iter()
            .map(|c| c.column)
            .collect::<Vec<Column>>();

        let insert_fields = match insert_fields {
            Some(fields) => fields.into_iter(),
            None => create_fields.into_iter(),
        };

        let insert_literals = insert_data
            .into_iter()
            .nth(0)
            .expect("data in insert_statement should have something")
            .into_iter();

        let items = insert_fields.zip(insert_literals).collect();
        let key = Uuid::new_v4().to_hyphenated().to_string();

        Row { key, items }
    }
}
