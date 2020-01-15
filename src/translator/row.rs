use nom_sql::{Column, CreateTableStatement, InsertStatement, Literal};
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

impl From<(CreateTableStatement, InsertStatement)> for Row {
    fn from((create_statement, insert_statement): (CreateTableStatement, InsertStatement)) -> Self {
        let create_fields = create_statement
            .fields
            .into_iter()
            .map(|c| c.column)
            .collect::<Vec<Column>>();

        let insert_fields = match insert_statement.fields {
            Some(fields) => fields.into_iter(),
            None => create_fields.into_iter(),
        };

        let insert_literals = insert_statement
            .data
            .into_iter()
            .nth(0)
            .expect("data in insert_statement should have something")
            .into_iter();

        let items = insert_fields.zip(insert_literals).collect();
        let key = Uuid::new_v4().to_hyphenated().to_string();

        Row { key, items }
    }
}
