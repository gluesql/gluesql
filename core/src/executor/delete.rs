use {
    super::{
        fetch::{fetch, fetch_columns},
        Payload, Referencing,
    },
    crate::{
        ast::{Assignment, BinaryOperator, Expr, ForeignKey},
        result::{Error, Result},
        store::{GStore, GStoreMut},
    },
    futures::stream::{StreamExt, TryStreamExt},
    serde::Serialize,
    std::rc::Rc,
    thiserror::Error as ThisError,
};

use crate::prelude::Key;
use futures::Future;
use std::pin::Pin;

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum DeleteError {
    #[error("Restrict reference column exists: {0}")]
    RestrictingColumnExists(String),

    #[error("Value not found on column: {0}")]
    ValueNotFound(String),

    #[error("Column does not have a default value: {0}")]
    ColumnDoesNotHaveDefaultValue(String),
}

type DeleteRows = (
    Vec<Key>,
    Vec<(
        Vec<(String, Expr)>,
        Vec<(String, String, Expr)>,
        Vec<(String, String, Expr)>,
    )>,
);

pub async fn delete<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    selection: &Option<Expr>,
) -> Result<Payload> {
    let columns = fetch_columns(storage, table_name).await?.map(Rc::from);
    let referencings = storage.fetch_referencings(table_name).await?;
    let keys_and_ops = fetch(storage, table_name, columns, selection.as_ref())
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
                        on_delete,
                        ..
                    },
            } in &referencings
            {
                let value = row
                    .get_value(referenced_column_name)
                    .ok_or(DeleteError::ValueNotFound(referenced_column_name.clone()))?
                    .clone();

                let expr = &Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(referencing_column_name.clone())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::try_from(value)?),
                };

                let columns = Some(Rc::from(Vec::new()));
                let referencing_rows =
                    fetch(storage, referencing_table_name, columns, Some(expr)).await?;

                let referencing_row_exists = Box::pin(referencing_rows).next().await.is_some();

                if referencing_row_exists {
                    use crate::ast::ReferentialAction::*;
                    match on_delete {
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
                            return Err(DeleteError::RestrictingColumnExists(format!(
                                "{referencing_table_name}.{referencing_column_name}"
                            ))
                            .into());
                        }
                    }
                }
            }
            Ok::<_, Error>((key, delete_ops, update_null_ops, update_default_ops))
        })
        .try_collect::<Vec<_>>()
        .await?;

    let (keys, ops): DeleteRows = keys_and_ops
        .into_iter()
        .map(|(key, delete_ops, update_null_ops, update_default_ops)| {
            (key, (delete_ops, update_null_ops, update_default_ops))
        })
        .unzip();

    let num_keys = keys.len();

    let mut deletion = storage
        .delete_data(table_name, keys)
        .await
        .map(|_| Payload::Delete(num_keys))?;

    for (delete_ops, update_null_ops, update_default_ops) in ops {
        for (referencing_table_name, expr) in delete_ops {
            let expr = Some(expr);
            let boxed_future: Pin<Box<dyn Future<Output = Result<Payload>>>> =
                Box::pin(delete(storage, &referencing_table_name, &expr));
            deletion.accumulate(&boxed_future.await?);
        }

        for (referencing_table_name, referencing_column_name, expr) in update_null_ops {
            let selection = Some(expr);
            let assignment = vec![Assignment::new(
                referencing_column_name.clone(),
                Expr::null(),
            )];
            let boxed_future: Pin<Box<dyn Future<Output = Result<Payload>>>> =
                Box::pin(super::update::update(
                    storage,
                    &referencing_table_name,
                    &selection,
                    assignment.as_slice(),
                ));

            boxed_future.await?;
        }

        for (referencing_table_name, referencing_column_name, expr) in update_default_ops {
            let selection: Option<Expr> = Some(expr);
            let column_defs = storage
                .fetch_schema(&referencing_table_name)
                .await?
                .and_then(|schema| schema.column_defs);

            let default = column_defs
                .as_ref()
                .and_then(|column_defs| {
                    column_defs
                        .iter()
                        .find(|column_def| column_def.name == referencing_column_name)
                        .and_then(|column_def| column_def.default.clone())
                })
                .ok_or(DeleteError::ColumnDoesNotHaveDefaultValue(
                    referencing_column_name.clone(),
                ))?;
            let assignment = vec![Assignment::new(referencing_column_name.clone(), default)];
            let boxed_future: Pin<Box<dyn Future<Output = Result<Payload>>>> =
                Box::pin(super::update::update(
                    storage,
                    &referencing_table_name,
                    &selection,
                    assignment.as_slice(),
                ));
            boxed_future.await?;
        }
    }

    Ok(deletion)
}
