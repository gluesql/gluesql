use {
    super::{
        context::RowContext,
        evaluate::{evaluate, Evaluated},
    },
    crate::{
        ast::{Assignment, ColumnDef, ForeignKey},
        data::{Key, Row, Schema, Value},
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

    #[error("cannot find referenced value on {table_name}.{column_name} with value {referenced_value:?}")]
    CannotFindReferencedValue {
        table_name: String,
        column_name: String,
        referenced_value: String,
    },
}

pub struct Update<'a, T: GStore> {
    storage: &'a T,
    table_name: &'a str,
    fields: &'a [Assignment],
    schema: &'a Schema,
}

impl<'a, T: GStore> Update<'a, T> {
    pub fn new(
        storage: &'a T,
        table_name: &'a str,
        fields: &'a [Assignment],
        schema: &'a Schema,
    ) -> Result<Self> {
        if schema.column_defs.is_some() {
            for assignment in fields.iter() {
                let Assignment { id, .. } = assignment;

                if !schema.has_column(id) {
                    return Err(UpdateError::ColumnNotFound(id.to_owned()).into());
                } else if schema.is_primary_key(id) {
                    return Err(UpdateError::UpdateOnPrimaryKeyNotSupported(id.to_owned()).into());
                }
            }
        }

        Ok(Self {
            storage,
            table_name,
            fields,
            schema,
        })
    }

    pub async fn apply(&self, row: Row, foreign_keys: &[ForeignKey]) -> Result<Row> {
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
                    let value = if self.schema.column_defs.is_some() {
                        let ColumnDef {
                            data_type,
                            nullable,
                            ..
                        } = self
                            .schema
                            .get_column_def(id)
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
                    } else {
                        evaluated.try_into()?
                    };

                    Ok::<_, Error>((id.as_ref(), value))
                }
            })
            .and_then(|(id, value)| async move {
                if value == Value::Null {
                    return Ok((id, value));
                }

                for foreign_key in foreign_keys {
                    let ForeignKey {
                        referencing_column_name,
                        referenced_table_name,
                        referenced_column_name,
                        ..
                    } = foreign_key;

                    if referencing_column_name != id {
                        continue;
                    }

                    let no_referenced = self
                        .storage
                        .fetch_data(referenced_table_name, &Key::try_from(&value)?)
                        .await?
                        .is_none();

                    if no_referenced {
                        return Err(UpdateError::CannotFindReferencedValue {
                            table_name: referenced_table_name.to_owned(),
                            column_name: referenced_column_name.to_owned(),
                            referenced_value: String::from(value),
                        }
                        .into());
                    }
                }

                Ok((id, value))
            })
            .try_collect::<Vec<(&str, Value)>>()
            .await?;

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
