use futures::stream::{self, TryStreamExt};
use serde::Serialize;
use std::fmt::Debug;
use std::rc::Rc;
use thiserror::Error as ThisError;

#[cfg(feature = "alter-table")]
use sqlparser::ast::AlterTableOperation;
use sqlparser::ast::{ObjectType, SetExpr, Statement, Values};

use crate::data::{get_name, Row, Schema};
use crate::parse_sql::Query;
use crate::result::{MutResult, Result};
use crate::store::{AlterTable, Index, Store, StoreMut};

use super::create_table::validate_table;
use super::fetch::{fetch, fetch_columns};
use super::filter::Filter;
use super::select::{select, select_with_labels};
use super::update::Update;
use super::validate::{validate_rows, ColumnValidation};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum ExecuteError {
    #[error("query not supported")]
    QueryNotSupported,

    #[error("drop type not supported")]
    DropTypeNotSupported,

    #[error("unsupported insert value type: {0}")]
    UnreachableUnsupportedInsertValueType(String),

    #[cfg(feature = "alter-table")]
    #[error("unsupported alter table operation: {0}")]
    UnsupportedAlterTableOperation(String),

    #[error("table does not exist")]
    TableNotExists,
}

#[derive(Serialize, Debug, PartialEq)]
pub enum Payload {
    Create,
    Insert(usize),
    Select {
        labels: Vec<String>,
        rows: Vec<Row>,
    },
    Delete(usize),
    Update(usize),
    DropTable,

    #[cfg(feature = "alter-table")]
    AlterTable,
}

pub async fn execute<
    T: 'static + Debug + Eq + Ord,
    U: Store<T> + StoreMut<T> + AlterTable + Index<T>,
>(
    storage: U,
    query: &Query,
) -> MutResult<U, Payload> {
    macro_rules! try_block {
        ($storage: expr, $block: block) => {{
            match (|| async { $block })().await {
                Err(e) => {
                    return Err(($storage, e));
                }
                Ok(v) => v,
            }
        }};
    }

    #[cfg(feature = "alter-table")]
    macro_rules! try_into {
        ($storage: expr, $expr: expr) => {
            match $expr {
                Err(e) => {
                    return Err(($storage, e.into()));
                }
                Ok(v) => v,
            }
        };
    }

    let Query(query) = query;

    match query {
        //- Modification
        //-- Tables
        Statement::CreateTable {
            name,
            columns,
            if_not_exists,
            ..
        } => {
            let schema = try_block!(storage, {
                let schema = Schema {
                    table_name: get_name(name)?.to_string(),
                    column_defs: columns.clone(),
                };

                validate_table(&storage, &schema, *if_not_exists).await?;
                Ok(schema)
            });

            storage
                .insert_schema(&schema)
                .await
                .map(|(storage, _)| (storage, Payload::Create))
        }
        Statement::Drop {
            object_type,
            names,
            if_exists,
            ..
        } => {
            if object_type != &ObjectType::Table {
                return Err((storage, ExecuteError::DropTypeNotSupported.into()));
            }

            stream::iter(names.iter().map(Ok))
                .try_fold(storage, |storage, table_name| async move {
                    let schema = try_block!(storage, {
                        let table_name = get_name(table_name)?;
                        let schema = storage.fetch_schema(table_name).await?;

                        if !if_exists {
                            schema.ok_or(ExecuteError::TableNotExists)?;
                        }
                        Ok(table_name)
                    });
                    storage
                        .delete_schema(schema)
                        .await
                        .map(|(storage, _)| storage)
                })
                .await
                .map(|storage| (storage, Payload::DropTable))
        }
        #[cfg(feature = "alter-table")]
        Statement::AlterTable { name, operation } => {
            let table_name = try_into!(storage, get_name(name));

            let result = match operation {
                AlterTableOperation::RenameTable {
                    table_name: new_table_name,
                } => {
                    let new_table_name = try_into!(storage, get_name(new_table_name));

                    storage.rename_schema(table_name, new_table_name).await
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    storage
                        .rename_column(table_name, &old_column_name.value, &new_column_name.value)
                        .await
                }
                AlterTableOperation::AddColumn { column_def } => {
                    storage.add_column(table_name, column_def).await
                }
                AlterTableOperation::DropColumn {
                    column_name,
                    if_exists,
                    ..
                } => {
                    storage
                        .drop_column(table_name, &column_name.value, *if_exists)
                        .await
                }
                _ => Err((
                    storage,
                    ExecuteError::UnsupportedAlterTableOperation(operation.to_string()).into(),
                )),
            };

            result.map(|(storage, _)| (storage, Payload::AlterTable))
        }

        //-- Rows
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => {
            let (rows, table_name) = try_block!(storage, {
                let table_name = get_name(table_name)?;
                let Schema { column_defs, .. } = storage
                    .fetch_schema(table_name)
                    .await?
                    .ok_or(ExecuteError::TableNotExists)?;

                let rows = match &source.body {
                    SetExpr::Values(Values(values_list)) => values_list
                        .iter()
                        .map(|values| Row::new(&column_defs, columns, values))
                        .collect::<Result<Vec<Row>>>()?,
                    SetExpr::Select(select_query) => {
                        let query = || sqlparser::ast::Query {
                            with: None,
                            body: SetExpr::Select(select_query.clone()),
                            order_by: vec![],
                            limit: None,
                            offset: None,
                            fetch: None,
                        };

                        select(&storage, &query(), None)
                            .await?
                            .try_collect::<Vec<_>>()
                            .await?
                    }
                    set_expr => {
                        return Err(ExecuteError::UnreachableUnsupportedInsertValueType(
                            set_expr.to_string(),
                        )
                        .into());
                    }
                };

                let column_defs = Rc::from(column_defs);
                validate_rows(
                    &storage,
                    &table_name,
                    ColumnValidation::All(Rc::clone(&column_defs)),
                    rows.iter(),
                )
                .await?;

                Ok((rows, table_name))
            });

            let num_rows = rows.len();

            storage
                .insert_data(table_name, rows)
                .await
                .map(|(storage, _)| (storage, Payload::Insert(num_rows)))
        }
        Statement::Update {
            table_name,
            selection,
            assignments,
        } => {
            let rows = try_block!(storage, {
                let table_name = get_name(table_name)?;
                let Schema { column_defs, .. } = storage
                    .fetch_schema(table_name)
                    .await?
                    .ok_or(ExecuteError::TableNotExists)?;
                let update = Update::new(&storage, table_name, assignments, &column_defs)?;
                let filter = Filter::new(&storage, selection.as_ref(), None, None);

                let all_columns = Rc::from(update.all_columns());
                let columns_to_update = update.columns_to_update();
                let rows = fetch(&storage, table_name, Rc::clone(&all_columns), filter)
                    .await?
                    .and_then(|item| {
                        let update = &update;
                        let (_, key, row) = item;
                        async move {
                            let row = update.apply(row).await?;
                            Ok((key, row))
                        }
                    })
                    .try_collect::<Vec<_>>()
                    .await?;

                let all_column_defs = Rc::from(column_defs);
                validate_rows(
                    &storage,
                    &table_name,
                    ColumnValidation::SpecifiedColumns(
                        Rc::clone(&all_column_defs),
                        columns_to_update,
                    ),
                    rows.iter().map(|r| &r.1),
                )
                .await?;
                Ok(rows)
            });
            let num_rows = rows.len();
            storage
                .update_data(rows)
                .await
                .map(|(storage, _)| (storage, Payload::Update(num_rows)))
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let keys = try_block!(storage, {
                let table_name = get_name(&table_name)?;
                let columns = Rc::from(fetch_columns(&storage, table_name).await?);
                let filter = Filter::new(&storage, selection.as_ref(), None, None);

                fetch(&storage, table_name, columns, filter)
                    .await?
                    .map_ok(|(_, key, _)| key)
                    .try_collect::<Vec<_>>()
                    .await
            });

            let num_keys = keys.len();

            storage
                .delete_data(keys)
                .await
                .map(|(storage, _)| (storage, Payload::Delete(num_keys)))
        }

        //- Selection
        Statement::Query(query) => {
            let (labels, rows) = try_block!(storage, {
                let (labels, rows) = select_with_labels(&storage, &query, None, true).await?;
                let rows = rows.try_collect::<Vec<_>>().await?;

                Ok((labels, rows))
            });

            Ok((storage, Payload::Select { labels, rows }))
        }
        _ => Err((storage, ExecuteError::QueryNotSupported.into())),
    }
}
