use futures::stream::{self, TryStreamExt};
use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Assignment, Ident};

use super::context::FilterContext;
use super::evaluate::{evaluate, Evaluated};
use crate::data::{Row, Value};
use crate::result::Result;
use crate::store::Store;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum UpdateError {
    #[error("column not found {0}")]
    ColumnNotFound(String),

    #[error("conflict on schema, row data does not fit to schema")]
    ConflictOnSchema,

    #[error("unreachable")]
    Unreachable,
}

pub struct Update<'a, T: 'static + Debug> {
    storage: &'a dyn Store<T>,
    table_name: &'a str,
    fields: &'a [Assignment],
    columns: Rc<Vec<Ident>>,
}

impl<'a, T: 'static + Debug> Update<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        table_name: &'a str,
        fields: &'a [Assignment],
        columns: Rc<Vec<Ident>>,
    ) -> Result<Self> {
        for assignment in fields.iter() {
            let Assignment { id, .. } = assignment;

            if columns.iter().all(|column| column.value != id.value) {
                return Err(UpdateError::ColumnNotFound(id.value.to_string()).into());
            }
        }

        Ok(Self {
            storage,
            table_name,
            fields,
            columns,
        })
    }

    async fn find(&self, row: &Row, column: &Ident) -> Result<Option<Value>> {
        let context =
            FilterContext::new(self.table_name, Rc::clone(&self.columns), Some(row), None);
        let context = Some(Rc::new(context));

        match self
            .fields
            .iter()
            .find(|assignment| assignment.id.value == column.value)
        {
            None => Ok(None),
            Some(assignment) => {
                let Assignment { id, value } = &assignment;

                let index = self
                    .columns
                    .iter()
                    .position(|column| column.value == id.value)
                    .ok_or_else(|| UpdateError::Unreachable)?;

                let evaluated = evaluate(self.storage, context, None, value, false).await?;

                let Row(values) = &row;
                let value = &values[index];

                match evaluated {
                    Evaluated::LiteralRef(v) => value.clone_by(v),
                    Evaluated::Literal(v) => value.clone_by(&v),
                    Evaluated::StringRef(v) => Ok(Value::Str(v.to_string())),
                    Evaluated::ValueRef(v) => Ok(v.clone()),
                    Evaluated::Value(v) => Ok(v),
                }
                .map(Some)
            }
        }
    }

    pub async fn apply(&self, row: Row) -> Result<Row> {
        let Row(values) = &row;

        let values = values.clone().into_iter().enumerate().map(|(i, value)| {
            self.columns
                .get(i)
                .map(|column| (column, value))
                .ok_or_else(|| UpdateError::ConflictOnSchema.into())
        });

        stream::iter(values)
            .and_then(|(column, value)| {
                let row = &row;

                async move {
                    self.find(row, column)
                        .await
                        .transpose()
                        .unwrap_or(Ok(value))
                }
            })
            .try_collect::<Vec<_>>()
            .await
            .map(Row)
    }
}
