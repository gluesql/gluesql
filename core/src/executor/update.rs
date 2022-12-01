use {
    super::{
        context::RowContext,
        evaluate::{evaluate, Evaluated},
    },
    crate::{
        ast::{Assignment, ColumnDef, ColumnOption},
        data::{Row, Value},
        result::Result,
        store::GStore,
    },
    futures::stream::{self, TryStreamExt},
    serde::Serialize,
    std::{fmt::Debug, rc::Rc},
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum UpdateError {
    #[error("column not found {0}")]
    ColumnNotFound(String),

    #[error("update on primary key is not supported: {0}")]
    UpdateOnPrimaryKeyNotSupported(String),

    #[error("conflict on schema, row data does not fit to schema")]
    ConflictOnSchema,
}

pub struct Update<'a> {
    storage: &'a dyn GStore,
    table_name: &'a str,
    fields: &'a [Assignment],
    column_defs: &'a [ColumnDef],
}

impl<'a> Update<'a> {
    pub fn new(
        storage: &'a dyn GStore,
        table_name: &'a str,
        fields: &'a [Assignment],
        column_defs: &'a [ColumnDef],
    ) -> Result<Self> {
        for assignment in fields.iter() {
            let Assignment { id, .. } = assignment;

            if column_defs.iter().all(|col_def| &col_def.name != id) {
                return Err(UpdateError::ColumnNotFound(id.to_owned()).into());
            } else if column_defs.iter().any(|ColumnDef { name, options, .. }| {
                if name != id {
                    return false;
                }

                options
                    .iter()
                    .any(|option| option == &ColumnOption::Unique { is_primary: true })
            }) {
                return Err(UpdateError::UpdateOnPrimaryKeyNotSupported(id.to_owned()).into());
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
        let context = RowContext::new(self.table_name, row, None);
        let context = Some(Rc::new(context));

        match self
            .fields
            .iter()
            .find(|assignment| assignment.id == column_def.name)
        {
            None => Ok(None),
            Some(assignment) => {
                let Assignment { value, .. } = &assignment;
                let ColumnDef {
                    data_type,
                    nullable,
                    ..
                } = column_def;

                let value = match evaluate(self.storage, context, None, value).await? {
                    Evaluated::Literal(v) => Value::try_from_literal(data_type, &v)?,
                    Evaluated::Value(v) => {
                        v.validate_type(data_type)?;
                        v
                    }
                };

                value.validate_null(*nullable)?;

                Ok(Some(value))
            }
        }
    }

    pub async fn apply(&self, row: Row) -> Result<Row> {
        let values = row
            .values
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
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
            .map(|values| Row {
                columns: Rc::clone(&row.columns),
                values,
            })
    }

    pub fn all_columns(&self) -> Vec<String> {
        self.column_defs
            .iter()
            .map(|col_def| col_def.name.to_owned())
            .collect()
    }

    pub fn columns_to_update(&self) -> Vec<String> {
        self.fields
            .iter()
            .map(|assignment| assignment.id.to_owned())
            .collect()
    }
}
