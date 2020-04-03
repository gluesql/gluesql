use nom_sql::{Column, ColumnSpecification, Literal};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

use crate::data::Value;
use crate::result::Result;

#[derive(Error, Debug, PartialEq)]
pub enum RowError {
    #[error("value not found")]
    ValueNotFound,

    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

    #[error("literals does not fit to columns")]
    LackOfRequiredValue(String),

    #[error("Unreachable")]
    Unreachable,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Row(pub Vec<Value>);

impl Row {
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.0.get(index)
    }

    pub fn take_first_value(self) -> Result<Value> {
        self.0
            .into_iter()
            .next()
            .ok_or(RowError::ValueNotFound.into())
    }

    pub fn new(
        create_fields: Vec<ColumnSpecification>,
        insert_fields: &Option<Vec<Column>>,
        insert_data: &Vec<Vec<Literal>>,
    ) -> Result<Self> {
        let insert_data = insert_data.first().ok_or(RowError::Unreachable)?;

        create_fields
            .into_iter()
            .map(|c| (c.sql_type, c.column))
            .enumerate()
            .map(|(i, (sql_type, column))| {
                let i = insert_fields.as_ref().map_or(Ok(i), |columns| {
                    columns
                        .iter()
                        .position(|target| target.name == column.name)
                        .ok_or(RowError::LackOfRequiredColumn(column.name.clone()))
                })?;

                let literal = insert_data
                    .get(i)
                    .ok_or(RowError::LackOfRequiredValue(column.name))?
                    .clone();

                Value::new(sql_type, literal)
            })
            .collect::<Result<_>>()
            .map(|items| Self(items))
    }
}
