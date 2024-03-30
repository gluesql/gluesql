use crate::{ast::ForeignKey, prelude::Key};

use super::ValidateError;

use {
    super::{
        context::RowContext,
        evaluate::{evaluate, Evaluated},
    },
    crate::{
        ast::{Assignment, ColumnDef, ColumnUniqueOption},
        data::{Row, Value},
        result::{Error, Result},
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStreamExt},
    serde::Serialize,
    std::{borrow::Cow, fmt::Debug, rc::Rc},
    thiserror::Error,
    utils::HashMapExt,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum UpdateError {
    #[error("column not found {0}")]
    ColumnNotFound(String),

    #[error("update on primary key is not supported: {0}")]
    UpdateOnPrimaryKeyNotSupported(String),

    #[error("conflict on schema, row data does not fit to schema")]
    ConflictOnSchema,
}

pub struct Update<'a, T: GStore> {
    storage: &'a T,
    table_name: &'a str,
    fields: &'a [Assignment],
    column_defs: Option<&'a [ColumnDef]>,
}

impl<'a, T: GStore> Update<'a, T> {
    pub fn new(
        storage: &'a T,
        table_name: &'a str,
        fields: &'a [Assignment],
        column_defs: Option<&'a [ColumnDef]>,
    ) -> Result<Self> {
        if let Some(column_defs) = column_defs {
            for assignment in fields.iter() {
                let Assignment { id, .. } = assignment;

                if column_defs.iter().all(|col_def| &col_def.name != id) {
                    return Err(UpdateError::ColumnNotFound(id.to_owned()).into());
                } else if column_defs.iter().any(|ColumnDef { name, unique, .. }| {
                    name == id && matches!(unique, Some(ColumnUniqueOption { is_primary: true }))
                }) {
                    return Err(UpdateError::UpdateOnPrimaryKeyNotSupported(id.to_owned()).into());
                }
            }
        }

        Ok(Self {
            storage,
            table_name,
            fields,
            column_defs,
        })
    }

    pub async fn apply(
        &self,
        row: Row,
        foreign_keys: Option<Vec<ForeignKey>>,
        table_name: &'a str,
    ) -> Result<Row> {
        let context = RowContext::new(self.table_name, Cow::Borrowed(&row), None);
        let context = Some(Rc::new(context));

        let assignments = stream::iter(self.fields.iter())
            .then(|assignment| {
                let Assignment {
                    id,
                    value: value_expr,
                } = assignment;
                let context = context.as_ref().map(Rc::clone);

                async move {
                    let evaluated = evaluate(self.storage, context, None, value_expr).await?;
                    let value = match self.column_defs {
                        Some(column_defs) => {
                            let ColumnDef {
                                data_type,
                                nullable,
                                ..
                            } = column_defs
                                .iter()
                                .find(|column_def| id == &column_def.name)
                                .ok_or(UpdateError::ConflictOnSchema)?;

                            let value = match evaluated {
                                Evaluated::Literal(v) => Value::try_from_literal(data_type, &v)?,
                                Evaluated::Value(v) => {
                                    v.validate_type(data_type)?;
                                    v
                                }
                                Evaluated::StrSlice {
                                    source: s,
                                    range: r,
                                } => Value::Str(s[r].to_owned()),
                            };

                            value.validate_null(*nullable)?;
                            value
                        }
                        None => evaluated.try_into()?,
                    };

                    Ok::<_, Error>((id.as_ref(), value))
                }
            })
            .try_collect::<Vec<(&str, Value)>>()
            .await?;

        for (id, value) in assignments.iter() {
            if let Some(foreign_keys) = &foreign_keys {
                for ForeignKey {
                    name,
                    column,
                    referred_table,
                    referred_column,
                    ..
                } in foreign_keys
                {
                    if column != id || value == &Value::Null {
                        continue;
                    }

                    let no_parent = self
                        .storage
                        .fetch_data(referred_table, &Key::try_from(value)?)
                        .await?
                        .is_none();

                    if no_parent {
                        return Err(ValidateError::ForeignKeyViolation {
                            name: name.to_owned(),
                            table: table_name.to_owned(),
                            column: column.to_owned(),
                            referred_table: referred_table.to_owned(),
                            referred_column: referred_column.to_owned(),
                        }
                        .into());
                    }
                }
            }
        }

        Ok(match row {
            Row::Vec { columns, values } => {
                let values = columns
                    .iter()
                    .zip(values)
                    .map(|(column, value)| {
                        assignments
                            .iter()
                            .find_map(|(id, new_value)| (column == id).then_some(new_value.clone()))
                            .unwrap_or(value)
                    })
                    .collect();

                Row::Vec { columns, values }
            }
            Row::Map(values) => {
                let assignments = assignments
                    .into_iter()
                    .map(|(id, value)| (id.to_owned(), value));

                Row::Map(values.concat(assignments))
            }
        })
    }
}
