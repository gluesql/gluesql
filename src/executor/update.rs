use futures::stream::{self, TryStreamExt};
use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Assignment, ColumnDef, Ident};

use super::context::FilterContext;
use super::evaluate::{evaluate, Evaluated};
use crate::data::value::TryFromLiteral;
use crate::data::{schema::ColumnDefExt, Row, Value};
use crate::result::Result;
use crate::store::Store;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum UpdateError {
    #[error("column not found {0}")]
    ColumnNotFound(String),

    #[error("conflict on schema, row data does not fit to schema")]
    ConflictOnSchema,
}

pub struct Update<'a, T: 'static + Debug> {
    storage: &'a dyn Store<T>,
    table_name: &'a str,
    fields: &'a [Assignment],
    column_defs: &'a [ColumnDef],
}

impl<'a, T: 'static + Debug> Update<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        table_name: &'a str,
        fields: &'a [Assignment],
        column_defs: &'a [ColumnDef],
    ) -> Result<Self> {
        for assignment in fields.iter() {
            let Assignment { id, .. } = assignment;

            if column_defs
                .iter()
                .all(|col_def| col_def.name.value != id.value)
            {
                return Err(UpdateError::ColumnNotFound(id.value.to_string()).into());
            }
        }

        Ok(Self {
            storage,
            table_name,
            fields,
            column_defs,
        })
    }

    async fn find(&self, row: &Row, column_def: &ColumnDef) -> Result<Option<Value>> {
        let all_columns = Rc::from(self.all_columns());
        let context = FilterContext::new(self.table_name, Rc::clone(&all_columns), Some(row), None);
        let context = Some(Rc::new(context));

        match self
            .fields
            .iter()
            .find(|assignment| assignment.id.value == column_def.name.value)
        {
            None => Ok(None),
            Some(assignment) => {
                let Assignment { value, .. } = &assignment;
                let ColumnDef { data_type, .. } = column_def;
                let nullable = column_def.is_nullable();

                let value = match evaluate(self.storage, context, None, value, false).await? {
                    Evaluated::Literal(v) => Value::try_from_literal(data_type, &v)?,
                    Evaluated::Value(v) => {
                        v.validate_type(data_type)?;
                        v.into_owned()
                    }
                };

                value.validate_null(nullable)?;

                Ok(Some(value))
            }
        }
    }

    pub async fn apply(&self, row: Row) -> Result<Row> {
        let Row(values) = &row;

        let values = values.clone().into_iter().enumerate().map(|(i, value)| {
            self.column_defs
                .get(i)
                .map(|col_def| (col_def, value))
                .ok_or_else(|| UpdateError::ConflictOnSchema.into())
        });

        stream::iter(values)
            .and_then(|(col_def, value)| {
                let row = &row;

                async move {
                    self.find(row, col_def)
                        .await
                        .transpose()
                        .unwrap_or(Ok(value))
                }
            })
            .try_collect::<Vec<_>>()
            .await
            .map(Row)
    }

    pub fn all_columns(&self) -> Vec<Ident> {
        self.column_defs
            .iter()
            .map(|col_def| col_def.name.clone())
            .collect()
    }

    pub fn columns_to_update(&self) -> Vec<Ident> {
        self.fields
            .iter()
            .map(|assignment| assignment.id.clone())
            .collect()
    }
}
