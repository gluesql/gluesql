use nom_sql::{Column, ColumnSpecification, Literal};
use serde::{Deserialize, Serialize};
use std::convert::From;
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize)]
pub struct Row<T: Debug> {
    pub key: T,
    pub items: Vec<(Column, Literal)>,
}

impl<T: Debug> Row<T> {
    pub fn get_literal(&self, target: &Column) -> Option<&Literal> {
        let column_name = &target.name;

        self.items
            .iter()
            .filter(|(column, _)| &column.name == column_name)
            .map(|(_, literal)| literal)
            .nth(0)
    }
}

impl<T: Debug> Row<T> {
    pub fn take_first_literal(row: Row<T>) -> Option<Literal> {
        row.items.into_iter().nth(0).map(|(_, literal)| literal)
    }
}

impl<'a, T: Debug>
    From<(
        T,
        Vec<ColumnSpecification>,
        &'a Option<Vec<Column>>,
        &'a Vec<Vec<Literal>>,
    )> for Row<T>
{
    fn from(
        (key, create_fields, insert_fields, insert_data): (
            T,
            Vec<ColumnSpecification>,
            &'a Option<Vec<Column>>,
            &'a Vec<Vec<Literal>>,
        ),
    ) -> Self {
        let create_fields = create_fields
            .into_iter()
            .map(|c| c.column)
            .collect::<Vec<Column>>();

        let insert_fields = match insert_fields {
            Some(fields) => fields.clone().into_iter(),
            None => create_fields.into_iter(),
        };

        let insert_literals = insert_data
            .clone()
            .into_iter()
            .nth(0)
            .expect("data in insert_statement should have something")
            .into_iter();

        let items = insert_fields.zip(insert_literals).collect();

        Row { key, items }
    }
}
