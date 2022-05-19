use {
    super::{
        alter::{create_table, drop_table},
        fetch::{fetch, fetch_columns},
        select::{select, select_with_labels},
        update::Update,
        validate::{validate_unique, ColumnValidation},
    },
    crate::{
        ast::{DataType, SetExpr, Statement, Values},
        data::{get_name, Row, Schema, SchemaIndexOrd, Value},
        executor::limit::Limit,
        result::MutResult,
        store::{GStore, GStoreMut},
    },
    futures::stream::{self, TryStreamExt},
    serde::Serialize,
    std::{fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[cfg(feature = "alter-table")]
use super::alter::alter_table;

#[cfg(feature = "index")]
use super::alter::{create_index, drop_index};

#[cfg(feature = "metadata")]
use crate::{ast::Variable, result::TrySelf};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum ExecuteError {
    #[error("table not found: {0}")]
    TableNotFound(String),
}

#[derive(Serialize, Debug, PartialEq)]
pub enum Payload {
    ShowColumns(Vec<(String, DataType)>),
    Create,
    Insert(usize),
    Select {
        labels: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Delete(usize),
    Update(usize),
    DropTable,

    #[cfg(feature = "alter-table")]
    AlterTable,

    #[cfg(feature = "index")]
    CreateIndex,
    #[cfg(feature = "index")]
    DropIndex,

    #[cfg(feature = "transaction")]
    StartTransaction,
    #[cfg(feature = "transaction")]
    Commit,
    #[cfg(feature = "transaction")]
    Rollback,

    #[cfg(feature = "metadata")]
    ShowVariable(PayloadVariable),
    ShowIndexes(Vec<(String, SchemaIndexOrd)>),
}

#[cfg(feature = "metadata")]
#[derive(Serialize, Debug, PartialEq)]
pub enum PayloadVariable {
    Tables(Vec<String>),
    Version(String),
}

#[cfg(feature = "transaction")]
pub async fn execute_atomic<T, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    statement: &Statement,
) -> MutResult<U, Payload> {
    if matches!(
        statement,
        Statement::StartTransaction | Statement::Rollback | Statement::Commit
    ) {
        return execute(storage, statement).await;
    }

    let (storage, autocommit) = storage.begin(true).await?;
    let result = execute(storage, statement).await;

    match (result, autocommit) {
        (Ok((storage, payload)), true) => {
            let (storage, ()) = storage.commit().await?;

            Ok((storage, payload))
        }
        (Err((storage, error)), true) => {
            let (storage, ()) = storage.rollback().await?;

            Err((storage, error))
        }
        (result, _) => result,
    }
}

pub async fn execute<T, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    statement: &Statement,
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

    match statement {
        //- Modification
        //-- Tables
        Statement::CreateTable {
            name,
            columns,
            if_not_exists,
            source,
            ..
        } => create_table(storage, name, columns, *if_not_exists, source)
            .await
            .map(|(storage, _)| (storage, Payload::Create)),
        Statement::DropTable {
            names, if_exists, ..
        } => drop_table(storage, names, *if_exists)
            .await
            .map(|(storage, _)| (storage, Payload::DropTable)),
        #[cfg(feature = "alter-table")]
        Statement::AlterTable { name, operation } => alter_table(storage, name, operation)
            .await
            .map(|(storage, _)| (storage, Payload::AlterTable)),
        #[cfg(feature = "index")]
        Statement::CreateIndex {
            name,
            table_name,
            column,
        } => create_index(storage, table_name, name, column)
            .await
            .map(|(storage, _)| (storage, Payload::CreateIndex)),
        #[cfg(feature = "index")]
        Statement::DropIndex { name, table_name } => drop_index(storage, table_name, name)
            .await
            .map(|(storage, _)| (storage, Payload::DropIndex)),
        //- Transaction
        #[cfg(feature = "transaction")]
        Statement::StartTransaction => storage
            .begin(false)
            .await
            .map(|(storage, _)| (storage, Payload::StartTransaction)),
        #[cfg(feature = "transaction")]
        Statement::Commit => storage
            .commit()
            .await
            .map(|(storage, _)| (storage, Payload::Commit)),
        #[cfg(feature = "transaction")]
        Statement::Rollback => storage
            .rollback()
            .await
            .map(|(storage, _)| (storage, Payload::Rollback)),
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
                    .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;
                let column_defs = Rc::from(column_defs);
                let column_validation = ColumnValidation::All(Rc::clone(&column_defs));

                let rows = match &source.body {
                    SetExpr::Values(Values(values_list)) => {
                        let limit = Limit::new(source.limit.as_ref(), source.offset.as_ref())?;
                        let rows = values_list
                            .iter()
                            .map(|values| Row::new(&column_defs, columns, values));
                        let rows = stream::iter(rows);
                        let rows = limit.apply(rows);
                        rows.try_collect::<Vec<_>>().await?
                    }
                    SetExpr::Select(_) => {
                        select(&storage, source, None)
                            .await?
                            .and_then(|row| {
                                let column_defs = Rc::clone(&column_defs);

                                async move {
                                    row.validate(&column_defs)?;

                                    Ok(row)
                                }
                            })
                            .try_collect::<Vec<_>>()
                            .await?
                    }
                };

                validate_unique(&storage, table_name, column_validation, rows.iter()).await?;

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
            let (table_name, rows) = try_block!(storage, {
                let table_name = get_name(table_name)?;
                let Schema { column_defs, .. } = storage
                    .fetch_schema(table_name)
                    .await?
                    .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;
                let update = Update::new(&storage, table_name, assignments, &column_defs)?;

                let all_columns = Rc::from(update.all_columns());
                let columns_to_update = update.columns_to_update();
                let rows = fetch(&storage, table_name, all_columns, selection.as_ref())
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

                let column_validation =
                    ColumnValidation::SpecifiedColumns(Rc::from(column_defs), columns_to_update);
                validate_unique(
                    &storage,
                    table_name,
                    column_validation,
                    rows.iter().map(|r| &r.1),
                )
                .await?;

                Ok((table_name, rows))
            });

            let num_rows = rows.len();

            storage
                .update_data(table_name, rows)
                .await
                .map(|(storage, _)| (storage, Payload::Update(num_rows)))
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let (table_name, keys) = try_block!(storage, {
                let table_name = get_name(table_name)?;
                let columns = Rc::from(fetch_columns(&storage, table_name).await?);

                let keys = fetch(&storage, table_name, columns, selection.as_ref())
                    .await?
                    .map_ok(|(_, key, _)| key)
                    .try_collect::<Vec<_>>()
                    .await?;

                Ok((table_name, keys))
            });

            let num_keys = keys.len();

            storage
                .delete_data(table_name, keys)
                .await
                .map(|(storage, _)| (storage, Payload::Delete(num_keys)))
        }

        //- Selection
        Statement::Query(query) => {
            let (labels, rows) = try_block!(storage, {
                let (labels, rows) = select_with_labels(&storage, query, None, true).await?;
                let rows = rows
                    .map_ok(|Row(values)| values)
                    .try_collect::<Vec<_>>()
                    .await?;

                Ok((labels, rows))
            });

            Ok((storage, Payload::Select { labels, rows }))
        }
        Statement::ShowColumns { table_name } => {
            let keys = try_block!(storage, {
                let table_name = get_name(table_name)?;
                let Schema { column_defs, .. } = storage
                    .fetch_schema(table_name)
                    .await?
                    .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

                Ok(column_defs)
            });

            let output: Vec<(String, DataType)> = keys
                .into_iter()
                .map(|key| (key.name, key.data_type))
                .collect();

            Ok((storage, Payload::ShowColumns(output)))
        }
        Statement::ShowIndexes(table_name) => {
            let table_name = match get_name(table_name) {
                Ok(table_name) => table_name,
                Err(e) => return Err((storage, e)),
            };

            let indexes = match storage.fetch_schema(table_name).await {
                Ok(Some(Schema { indexes, .. })) => indexes,
                Ok(None) => {
                    return Err((
                        storage,
                        ExecuteError::TableNotFound(table_name.to_owned()).into(),
                    ));
                }
                Err(e) => return Err((storage, e)),
            };

            Ok((
                storage,
                Payload::ShowIndexes(
                    indexes
                        .into_iter()
                        .map(|index| (index.name, index.order))
                        .collect(),
                ),
            ))
        }
        //- Metadata
        #[cfg(feature = "metadata")]
        Statement::ShowVariable(variable) => match variable {
            Variable::Tables => storage
                .schema_names()
                .await
                .map(|table_names| Payload::ShowVariable(PayloadVariable::Tables(table_names)))
                .try_self(storage),
            Variable::Version => {
                let version = storage.version();
                let payload = Payload::ShowVariable(PayloadVariable::Version(version));

                Ok((storage, payload))
            }
        },
    }
}
