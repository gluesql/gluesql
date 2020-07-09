use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::{Assignment, Ident};

use crate::data::{Row, Value};
use crate::executor::{evaluate, Evaluated, FilterContext};
use crate::result::Result;
use crate::storage::Store;

#[derive(Error, Debug, PartialEq)]
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
    columns: &'a [Ident],
}

impl<'a, T: 'static + Debug> Update<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        table_name: &'a str,
        fields: &'a [Assignment],
        columns: &'a [Ident],
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

    fn find(&self, row: &Row, column: &Ident) -> Option<Result<Value>> {
        let context = FilterContext::new(self.table_name, self.columns, row, None);

        self.fields
            .iter()
            .find(|assignment| assignment.id.value == column.value)
            .map(|assignment| {
                let Assignment { id, value } = &assignment;

                let index = self
                    .columns
                    .iter()
                    .position(|column| column.value == id.value)
                    .ok_or_else(|| UpdateError::Unreachable)?;

                let evaluated = evaluate(self.storage, &context, value)?;
                let Row(values) = &row;
                let value = &values[index];

                match evaluated {
                    Evaluated::LiteralRef(v) => value.clone_by(v),
                    Evaluated::Literal(v) => value.clone_by(&v),
                    Evaluated::StringRef(v) => Ok(Value::Str(v.to_string())),
                    Evaluated::ValueRef(v) => Ok(v.clone()),
                    Evaluated::Value(v) => Ok(v),
                }
            })
    }

    pub fn apply(&self, row: Row) -> Result<Row> {
        let Row(values) = &row;

        values
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                let column = &self
                    .columns
                    .get(i)
                    .ok_or_else(|| UpdateError::ConflictOnSchema)?;

                self.find(&row, column).unwrap_or(Ok(value))
            })
            .collect::<Result<_>>()
            .map(Row)
    }
}
