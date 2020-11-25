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
use crate::result::{Error, MutResult, Result};
use crate::store::{AlterTable, Store, StoreMut};

use super::fetch::{fetch, fetch_columns};
use super::filter::Filter;
use super::select::{select, select_with_labels};
use super::update::Update;

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
    #[error("table already exists")]
    TableAlreadyExists,
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

pub async fn execute<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable>(
    storage: U,
    query: &Query,
) -> MutResult<U, Payload> {
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
    let prepared = prepare(&storage, query).await;
    let prepared = try_into!(storage, prepared);

    match prepared {
        Prepared::Create {
            schema,
            if_not_exists,
        } => {
            if try_into!(storage, storage.fetch_schema(&schema.table_name).await).is_some() {
                return if if_not_exists {
                    Ok((storage, Payload::Create))
                } else {
                    Err((storage, ExecuteError::TableAlreadyExists.into()))
                };
            }

            storage
                .insert_schema(&schema)
                .await
                .map(|(storage, _)| (storage, Payload::Create))
        }
        Prepared::Insert(table_name, rows) => {
            stream::iter(rows.into_iter().map(Ok::<Row, (U, Error)>))
                .try_fold((storage, 0), |(storage, num), row| async move {
                    let (storage, key) = storage.generate_id(&table_name).await?;
                    let (storage, _) = storage.insert_data(&key, row).await?;

                    Ok((storage, num + 1))
                })
                .await
                .map(|(storage, num_rows)| (storage, Payload::Insert(num_rows)))
        }
        Prepared::Delete(keys) => stream::iter(keys.into_iter().map(Ok::<T, (U, Error)>))
            .try_fold((storage, 0), |(storage, num), key| async move {
                let (storage, _) = storage.delete_data(&key).await?;

                Ok((storage, num + 1))
            })
            .await
            .map(|(storage, num_rows)| (storage, Payload::Delete(num_rows))),
        Prepared::Update(items) => stream::iter(items.into_iter().map(Ok::<_, (U, Error)>))
            .try_fold((storage, 0), |(storage, num), (key, row)| async move {
                let (storage, _) = storage.insert_data(&key, row).await?;

                Ok((storage, num + 1))
            })
            .await
            .map(|(storage, num_rows)| (storage, Payload::Update(num_rows))),
        Prepared::DropTable {
            schema_names,
            if_exists,
        } => stream::iter(schema_names.into_iter().map(Ok::<&str, (U, Error)>))
            .try_fold(storage, |storage, table_name| async move {
                let schema = try_into!(storage, storage.fetch_schema(table_name).await);

                if !if_exists {
                    try_into!(storage, schema.ok_or(ExecuteError::TableNotExists));
                }

                storage
                    .delete_schema(table_name)
                    .await
                    .map(|(storage, _)| storage)
            })
            .await
            .map(|storage| (storage, Payload::DropTable)),
        Prepared::Select { labels, rows } => Ok((storage, Payload::Select { labels, rows })),

        #[cfg(feature = "alter-table")]
        Prepared::AlterTable(table_name, operation) => {
            let result = match operation {
                AlterTableOperation::RenameTable {
                    table_name: new_table_name,
                } => {
                    storage
                        .rename_schema(table_name, &new_table_name.value)
                        .await
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
    }
}

enum Prepared<'a, T> {
    Create {
        schema: Schema,
        if_not_exists: bool,
    },
    Insert(&'a str, Vec<Row>),
    Delete(Vec<T>),
    Update(Vec<(T, Row)>),
    Select {
        labels: Vec<String>,
        rows: Vec<Row>,
    },
    DropTable {
        schema_names: Vec<&'a str>,
        if_exists: bool,
    },

    #[cfg(feature = "alter-table")]
    AlterTable(&'a str, &'a AlterTableOperation),
}

// looks like... false positive
#[allow(clippy::needless_lifetimes)]
async fn prepare<'a, T: 'static + Debug>(
    storage: &impl Store<T>,
    sql_query: &'a Statement,
) -> Result<Prepared<'a, T>> {
    match sql_query {
        Statement::CreateTable {
            name,
            columns,
            if_not_exists,
            ..
        } => {
            let schema = Schema {
                table_name: get_name(name)?.clone(),
                column_defs: columns.clone(),
            };

            Ok(Prepared::Create {
                schema,
                if_not_exists: *if_not_exists,
            })
        }
        Statement::Query(query) => {
            let (labels, rows) = select_with_labels(storage, &query, None, true).await?;
            let rows = rows.try_collect::<Vec<_>>().await?;

            Ok(Prepared::Select { labels, rows })
        }
        Statement::Insert {
            table_name,
            columns,
            source,
        } => {
            let table_name = get_name(table_name)?;
            let Schema { column_defs, .. } = storage
                .fetch_schema(table_name)
                .await?
                .ok_or(ExecuteError::TableNotExists)?;

            let rows = match &source.body {
                SetExpr::Values(Values(values_list)) => values_list
                    .iter()
                    .map(|values| Row::new(&column_defs, columns, values))
                    .collect::<Result<_>>()?,
                SetExpr::Select(select_query) => {
                    select(
                        storage,
                        &sqlparser::ast::Query {
                            ctes: vec![],
                            body: SetExpr::Select(select_query.clone()),
                            order_by: vec![],
                            limit: None,
                            offset: None,
                            fetch: None,
                        },
                        None,
                    )
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

            Ok(Prepared::Insert(table_name, rows))
        }
        Statement::Update {
            table_name,
            selection,
            assignments,
        } => {
            let table_name = get_name(table_name)?;
            let columns = Rc::new(fetch_columns(storage, table_name).await?);
            let update = Update::new(storage, table_name, assignments, Rc::clone(&columns))?;
            let filter = Filter::new(storage, selection.as_ref(), None, None);

            fetch(storage, table_name, columns, filter)
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
                .await
                .map(Prepared::Update)
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let table_name = get_name(table_name)?;
            let columns = Rc::new(fetch_columns(storage, table_name).await?);
            let filter = Filter::new(storage, selection.as_ref(), None, None);

            fetch(storage, table_name, columns, filter)
                .await?
                .map_ok(|(_, key, _)| key)
                .try_collect::<Vec<_>>()
                .await
                .map(Prepared::Delete)
        }
        Statement::Drop {
            object_type,
            names,
            if_exists,
            ..
        } => {
            if object_type != &ObjectType::Table {
                return Err(ExecuteError::DropTypeNotSupported.into());
            }

            let names = names
                .iter()
                .map(|name| get_name(name).map(|n| n.as_str()))
                .collect::<Result<_>>()?;

            Ok(Prepared::DropTable {
                schema_names: names,
                if_exists: *if_exists,
            })
        }

        #[cfg(feature = "alter-table")]
        Statement::AlterTable { name, operation } => {
            let table_name = get_name(name)?;

            Ok(Prepared::AlterTable(table_name, operation))
        }

        _ => Err(ExecuteError::QueryNotSupported.into()),
    }
}
