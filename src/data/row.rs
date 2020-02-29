use crate::data::Value;
use nom_sql::{Column, ColumnSpecification, Literal, SqlType};
use serde::{Deserialize, Serialize};
use std::convert::From;
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize)]
pub struct Row<T: Debug> {
    pub key: T,
    pub items: Vec<(Column, Value)>,
}

impl<T: Debug> Row<T> {
    pub fn get_value(&self, target: &Column) -> Option<&Value> {
        let column_name = &target.name;

        self.items
            .iter()
            .filter(|(column, _)| &column.name == column_name)
            .map(|(_, value)| value)
            .nth(0)
    }
}

impl<T: Debug> Row<T> {
    pub fn take_first_value(row: Row<T>) -> Option<Value> {
        row.items.into_iter().nth(0).map(|(_, value)| value)
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
            .map(|c| (c.sql_type, c.column))
            .collect::<Vec<(SqlType, Column)>>();

        // TODO: Should not depend on the "order" of insert_fields, but currently it is.
        assert_eq!(
            create_fields
                .iter()
                .map(|(_, column)| &column.name)
                .collect::<Vec<&String>>(),
            insert_fields
                .as_ref()
                .unwrap()
                .iter()
                .map(|column| &column.name)
                .collect::<Vec<&String>>(),
        );

        let insert_literals = insert_data
            .clone()
            .into_iter()
            .nth(0)
            .expect("data in insert_statement should have something")
            .into_iter();

        let items = create_fields
            .into_iter()
            .zip(insert_literals)
            .map(|((sql_type, column), literal)| (column, Value::from((sql_type, literal))))
            .collect();

        Row { key, items }
    }
}
