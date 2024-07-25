use super::delete::delete;
use super::Referencing;
use crate::ast::Expr;
use crate::error::ExecuteError;
use crate::executor::fetch::fetch;
use crate::executor::validate::validate_unique;
use crate::executor::validate::ColumnValidation;
use crate::prelude::Payload;
use crate::store::DataRow;
use futures::Future;
use std::pin::Pin;

use {
    super::{
        context::RowContext,
        evaluate::{evaluate, Evaluated},
    },
    crate::{
        ast::{Assignment, ColumnDef, ForeignKey},
        data::{Key, Row, Schema, Value},
        result::{Error, Result},
        store::{GStore, GStoreMut},
    },
    futures::stream::{self, StreamExt, TryStreamExt},
    serde::Serialize,
    std::{borrow::Cow, fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
    utils::HashMapExt,
};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
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

    #[error("Restrict reference column exists: {0}")]
    RestrictingColumnExists(String),

    #[error("Default value not found on column: {0}")]
    ColumnDoesNotHaveDefaultValue(String),
}

type UpdateRows = (
    Vec<(Key, DataRow)>,
    Vec<(
        Vec<(String, Expr)>,
        Vec<(String, String, Expr)>,
        Vec<(String, String, Expr)>,
    )>,
);

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

/// Update data in the table
///
/// # Arguments
/// * `storage` - The storage to execute the query
/// * `table_name` - The name of the table to update
/// * `selection` - The selection to filter the rows to update
/// * `assignments` - The assignments to update
pub async fn update<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    selection: &Option<Expr>,
    assignments: &[Assignment],
) -> Result<Payload> {
    let schema = storage
        .fetch_schema(table_name)
        .await?
        .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

    let all_columns = schema.column_defs.as_deref().map(|columns| {
        columns
            .iter()
            .map(|col_def| col_def.name.to_owned())
            .collect()
    });
    let columns_to_update: Vec<String> = assignments
        .iter()
        .map(|assignment| assignment.id.to_owned())
        .collect();

    let update_executor = Update::new(storage, table_name, assignments, &schema)?;

    let foreign_keys = Rc::new(&schema.foreign_keys);
    let referencings = storage.fetch_referencings(table_name).await?;

    let rows = fetch(storage, table_name, all_columns, selection.as_ref())
        .await?
        .into_stream()
        .then(|item| async {
            let (key, row) = item?;
            let mut delete_ops = Vec::new();
            let mut update_null_ops = Vec::new();
            let mut update_default_ops = Vec::new();

            for Referencing {
                table_name: referencing_table_name,
                foreign_key:
                    ForeignKey {
                        referencing_column_name,
                        referenced_column_name,
                        on_update,
                        ..
                    },
            } in &referencings
            {
                let value = row.get_value(referenced_column_name).unwrap().clone();

                let expr = &Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(referencing_column_name.clone())),
                    op: crate::ast::BinaryOperator::Eq,
                    right: Box::new(Expr::try_from(value)?),
                };

                let columns = Some(Rc::from(Vec::new()));
                let referencing_rows =
                    fetch(storage, referencing_table_name, columns, Some(expr)).await?;

                let referencing_row_exists = Box::pin(referencing_rows).next().await.is_some();

                if referencing_row_exists {
                    use crate::ast::ReferentialAction::*;
                    match on_update {
                        Cascade => {
                            delete_ops.push((referencing_table_name.clone(), expr.clone()));
                        }
                        SetNull => {
                            update_null_ops.push((
                                referencing_table_name.clone(),
                                referencing_column_name.clone(),
                                expr.clone(),
                            ));
                        }
                        SetDefault => {
                            update_default_ops.push((
                                referencing_table_name.clone(),
                                referencing_column_name.clone(),
                                expr.clone(),
                            ));
                        }
                        NoAction => {
                            return Err(UpdateError::RestrictingColumnExists(format!(
                                "{referencing_table_name}.{referencing_column_name}"
                            ))
                            .into());
                        }
                    }
                }
            }

            let foreign_keys = Rc::clone(&foreign_keys);
            let row = update_executor.apply(row, foreign_keys.as_ref()).await?;

            Ok::<_, Error>((key, row, delete_ops, update_null_ops, update_default_ops))
        })
        .try_collect::<Vec<_>>()
        .await?;

    if schema.column_defs.is_some() {
        let column_validation = ColumnValidation::SpecifiedColumns(columns_to_update);
        let rows = rows.iter().filter_map(|(_, row, _, _, _)| match row {
            Row::Vec { values, .. } => Some(values.as_slice()),
            Row::Map(_) => None,
        });

        validate_unique(storage, table_name, &schema, column_validation, rows).await?;
    }

    let num_rows = rows.len();
    let (rows, ops): UpdateRows = rows
        .into_iter()
        .map(
            |(key, row, delete_ops, update_null_ops, update_default_ops)| {
                (
                    (key, row.into()),
                    (delete_ops, update_null_ops, update_default_ops),
                )
            },
        )
        .unzip();

    let mut update_payload = storage
        .insert_data(table_name, rows)
        .await
        .map(|_| Payload::Update(num_rows))?;

    for (delete_ops, update_null_ops, update_default_ops) in ops {
        for (referencing_table_name, expr) in delete_ops {
            let expr = Some(expr);
            delete(storage, &referencing_table_name, &expr).await?;
        }

        for (referencing_table_name, referencing_column_name, expr) in update_null_ops {
            let expr = Some(expr);
            let assignment = vec![Assignment::new(referencing_column_name, Expr::null())];
            let boxed_update: Pin<Box<dyn Future<Output = Result<Payload>>>> =
                Box::pin(update(storage, &referencing_table_name, &expr, &assignment));
            update_payload.accumulate(&boxed_update.await?);
        }

        for (referencing_table_name, referencing_column_name, expr) in update_default_ops {
            let expr = Some(expr);
            let column_defs = storage
                .fetch_schema(&referencing_table_name)
                .await?
                .and_then(|schema| schema.column_defs);

            let default = vec![column_defs
                .as_ref()
                .and_then(|column_defs| {
                    column_defs
                        .iter()
                        .find(|column_def| column_def.name == referencing_column_name)
                        .and_then(|column_def| column_def.default.clone())
                })
                .map(|default| Assignment::new(referencing_column_name.clone(), default))
                .ok_or(UpdateError::ColumnDoesNotHaveDefaultValue(
                    referencing_column_name,
                ))?];

            let boxed_update: Pin<Box<dyn Future<Output = Result<Payload>>>> =
                Box::pin(update(storage, &referencing_table_name, &expr, &default));
            update_payload.accumulate(&boxed_update.await?);
        }
    }

    Ok(update_payload)
}
