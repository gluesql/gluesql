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
use crate::store::{AlterTable, Store, StoreMut};

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

pub async fn execute<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable + Clone>(
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

    match query {
        //- Modification
        //-- Tables
        Statement::CreateTable {
            name,
            columns,
            if_not_exists,
            ..
        } => {
            let schema = Schema {
                table_name: try_into!(storage, get_name(name)).clone(),
                column_defs: columns.clone(),
            };

            let validated = validate_table(&storage, &schema, *if_not_exists).await;
            try_into!(storage, validated);

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

            let schema_names: Vec<Result<&String>> = names
                .iter()
                .map(|name| get_name(name))
                .collect::<Vec<Result<&String>>>();

            stream::iter(schema_names.into_iter().map(|name| match name {
                Ok(name) => Ok(name.as_str()),
                Err(err) => Err((storage.clone(), err)),
            }))
            .try_fold(storage.clone(), |storage, table_name| async move {
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
            let table_name = try_into!(storage, get_name(table_name));
            let Schema { column_defs, .. } = try_into!(
                storage,
                try_into!(storage, storage.fetch_schema(table_name).await)
                    .ok_or(ExecuteError::TableNotExists)
            );

            let rows = match &source.body {
                SetExpr::Values(Values(values_list)) => try_into!(
                    storage,
                    values_list
                        .iter()
                        .map(|values| Row::new(&column_defs, columns, values))
                        .collect::<Result<_>>()
                ),
                SetExpr::Select(select_query) => {
                    try_into!(
                        storage.clone(),
                        try_into!(
                            storage.clone(),
                            select(
                                &storage,
                                &sqlparser::ast::Query {
                                    with: None,
                                    body: SetExpr::Select(select_query.clone()),
                                    order_by: vec![],
                                    limit: None,
                                    offset: None,
                                    fetch: None,
                                },
                                None,
                            )
                            .await
                        )
                        .try_collect::<Vec<_>>()
                        .await
                    )
                }
                set_expr => {
                    return Err((
                        storage,
                        ExecuteError::UnreachableUnsupportedInsertValueType(set_expr.to_string())
                            .into(),
                    ));
                }
            };

            let column_defs = Rc::from(column_defs);
            let validated = validate_rows(
                &storage,
                &table_name,
                ColumnValidation::All(Rc::clone(&column_defs)),
                rows.iter(),
            )
            .await;
            try_into!(storage, validated);

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
            let table_name = try_into!(storage, get_name(table_name));
            let Schema { column_defs, .. } = try_into!(
                storage,
                try_into!(storage, storage.fetch_schema(table_name).await)
                    .ok_or(ExecuteError::TableNotExists)
            );
            let update = try_into!(
                storage,
                Update::new(&storage, table_name, assignments, &column_defs)
            );
            let filter = Filter::new(&storage, selection.as_ref(), None, None);

            let all_columns = Rc::from(update.all_columns());
            let columns_to_update = update.columns_to_update();
            let rows = try_into!(
                storage.clone(),
                try_into!(
                    storage.clone(),
                    fetch(&storage, table_name, Rc::clone(&all_columns), filter).await
                )
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
            );

            let all_column_defs = Rc::from(column_defs);
            let validated = validate_rows(
                &storage,
                &table_name,
                ColumnValidation::SpecifiedColumns(Rc::clone(&all_column_defs), columns_to_update),
                rows.iter().map(|r| &r.1),
            )
            .await;
            try_into!(storage, validated);
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
            let table_name = try_into!(storage, get_name(&table_name));
            let columns = Rc::from(try_into!(
                storage,
                fetch_columns(&storage, table_name).await
            ));
            let filter = Filter::new(&storage, selection.as_ref(), None, None);

            let keys = try_into!(
                storage.clone(),
                try_into!(
                    storage.clone(),
                    fetch(&storage, table_name, columns, filter).await
                )
                .map_ok(|(_, key, _)| key)
                .try_collect::<Vec<_>>()
                .await
            );

            let num_rows = keys.len();

            storage
                .delete_data(keys)
                .await
                .map(|(storage, _)| (storage, Payload::Delete(num_rows)))
        }

        //- Selection
        Statement::Query(query) => {
            let (labels, rows) = try_into!(
                storage.clone(),
                select_with_labels(&storage, &query, None, true).await
            );
            let rows = try_into!(storage, rows.try_collect::<Vec<_>>().await);

            Ok((storage, Payload::Select { labels, rows }))
        }
        _ => Err((storage, ExecuteError::QueryNotSupported.into())),
    }
}
