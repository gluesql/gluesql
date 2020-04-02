use nom_sql::{Column, ColumnSpecification, Literal};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

use crate::data::Value;
use crate::ensure;
use crate::result::{Error, Result};

#[derive(Error, Debug, PartialEq)]
pub enum RowError {
    #[error("value not found")]
    ValueNotFound,
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
        let create_fields = create_fields
            .into_iter()
            .map(|c| (c.sql_type, c.column))
            .collect::<Vec<_>>();

        // TODO: Should not depend on the "order" of insert_fields, but currently it is.
        ensure!(
            create_fields
                .iter()
                .map(|(_, column)| &column.name)
                .collect::<Vec<_>>()
                == insert_fields
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|column| &column.name)
                    .collect::<Vec<_>>(),
            "create_fields do not match with insert_fields"
        );

        ensure!(!insert_data.is_empty(), "insert_data is empty");
        let insert_literals = insert_data[0].clone().into_iter();

        let items = create_fields
            .into_iter()
            .zip(insert_literals)
            .map(|((sql_type, _), literal)| Value::new(sql_type, literal))
            .collect::<Result<_>>()?;

        Ok(Self(items))
    }
}
