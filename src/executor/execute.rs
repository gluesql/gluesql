use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::{ObjectType, Statement};

use super::fetch::{fetch, fetch_columns};
use super::filter::Filter;
use super::select::select;
use super::update::Update;
use crate::data::{get_name, Row, Schema};
use crate::parse::Query;
use crate::result::{MutResult, Result};
use crate::store::{Store, StoreMut};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum ExecuteError {
    #[error("query not supported")]
    QueryNotSupported,

    #[error("drop type not supported")]
    DropTypeNotSupported,
}

#[derive(Serialize, Debug, PartialEq)]
pub enum Payload {
    Create,
    Insert(usize),
    Select(Vec<Row>),
    Delete(usize),
    Update(usize),
    DropTable,
}

pub fn execute<T: 'static + Debug, U: Store<T> + StoreMut<T>>(
    storage: U,
    query: &Query,
) -> MutResult<U, Payload> {
    let Query(query) = query;
    let prepared = match prepare(&storage, query) {
        Ok(prepared) => prepared,
        Err(error) => {
            return Err((storage, error));
        }
    };

    match prepared {
        Prepared::Create(schema) => storage
            .insert_schema(&schema)
            .map(|(storage, _)| (storage, Payload::Create)),
        Prepared::Insert(table_name, rows) => rows
            .into_iter()
            .try_fold((storage, 0), |(storage, num), row| {
                let (storage, key) = storage.generate_id(&table_name)?;
                let (storage, _) = storage.insert_data(&key, row)?;

                Ok((storage, num + 1))
            })
            .map(|(storage, num_rows)| (storage, Payload::Insert(num_rows))),
        Prepared::Delete(keys) => {
            let (storage, num_rows) =
                keys.into_iter()
                    .try_fold((storage, 0), |(storage, num), key| {
                        let (storage, _) = storage.delete_data(&key)?;

                        Ok((storage, num + 1))
                    })?;

            Ok((storage, Payload::Delete(num_rows)))
        }
        Prepared::Update(items) => {
            let (storage, num_rows) =
                items
                    .into_iter()
                    .try_fold((storage, 0), |(storage, num), item| {
                        let (key, row) = item;
                        let (storage, _) = storage.insert_data(&key, row)?;

                        Ok((storage, num + 1))
                    })?;

            Ok((storage, Payload::Update(num_rows)))
        }
        Prepared::DropTable(table_names) => {
            let storage = table_names
                .iter()
                .try_fold(storage, |storage, table_name| {
                    let (storage, _) = storage.delete_schema(table_name)?;

                    Ok(storage)
                })?;

            Ok((storage, Payload::DropTable))
        }
        Prepared::Select(rows) => Ok((storage, Payload::Select(rows))),
    }
}

enum Prepared<'a, T> {
    Create(Schema),
    Insert(&'a str, Vec<Row>),
    Delete(Vec<T>),
    Update(Vec<(T, Row)>),
    Select(Vec<Row>),
    DropTable(Vec<&'a str>),
}

fn prepare<'a, T: 'static + Debug>(
    storage: &impl Store<T>,
    sql_query: &'a Statement,
) -> Result<Prepared<'a, T>> {
    match sql_query {
        Statement::CreateTable { name, columns, .. } => {
            let schema = Schema {
                table_name: get_name(name)?.clone(),
                column_defs: columns.clone(),
            };

            Ok(Prepared::Create(schema))
        }
        Statement::Query(query) => {
            let rows = select(storage, &query, None)?.collect::<Result<_>>()?;

            Ok(Prepared::Select(rows))
        }
        Statement::Insert {
            table_name,
            columns,
            source,
        } => {
            let table_name = get_name(table_name)?;
            let Schema { column_defs, .. } = storage.fetch_schema(table_name)?;
            let rows = Row::new(column_defs, columns, source)?;

            Ok(Prepared::Insert(table_name, rows))
        }
        Statement::Update {
            table_name,
            selection,
            assignments,
        } => {
            let table_name = get_name(table_name)?;
            let columns = fetch_columns(storage, table_name)?;
            let update = Update::new(storage, table_name, assignments, &columns)?;
            let filter = Filter::new(storage, selection.as_ref(), None);

            let rows = fetch(storage, table_name, &columns, filter)?
                .map(|item| {
                    let (_, key, row) = item?;

                    Ok((key, update.apply(row)?))
                })
                .collect::<Result<_>>()?;

            Ok(Prepared::Update(rows))
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let table_name = get_name(table_name)?;
            let columns = fetch_columns(storage, table_name)?;
            let filter = Filter::new(storage, selection.as_ref(), None);

            let rows = fetch(storage, table_name, &columns, filter)?
                .map(|item| item.map(|(_, key, _)| key))
                .collect::<Result<_>>()?;

            Ok(Prepared::Delete(rows))
        }
        Statement::Drop {
            object_type, names, ..
        } => {
            if object_type != &ObjectType::Table {
                return Err(ExecuteError::DropTypeNotSupported.into());
            }

            let names = names
                .iter()
                .map(|name| get_name(name).map(|n| n.as_str()))
                .collect::<Result<_>>()?;

            Ok(Prepared::DropTable(names))
        }
        _ => Err(ExecuteError::QueryNotSupported.into()),
    }
}
