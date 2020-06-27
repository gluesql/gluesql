use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::Statement;

use crate::data::{get_table_name, Row, Schema};
use crate::executor::{fetch, fetch_columns, select, Filter, Update};
use crate::result::Result;
use crate::storage::Store;

#[derive(Error, Debug, PartialEq)]
pub enum ExecuteError {
    #[error("query not supported")]
    QueryNotSupported,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Payload {
    Create,
    Insert(Row),
    Select(Vec<Row>),
    Delete(usize),
    Update(usize),
}

pub fn execute<T: 'static + Debug>(
    storage: &dyn Store<T>,
    sql_query: &Statement,
) -> Result<Payload> {
    match sql_query {
        Statement::CreateTable { name, columns, .. } => {
            let schema = Schema {
                table_name: get_table_name(name)?.clone(),
                column_defs: columns.clone(),
            };

            storage.set_schema(&schema)?;

            Ok(Payload::Create)
        }
        Statement::Query(query) => {
            let rows = select(storage, &query, None)?.collect::<Result<_>>()?;

            Ok(Payload::Select(rows))
        }
        Statement::Insert {
            table_name,
            columns,
            source,
        } => {
            let table_name = get_table_name(table_name)?;
            let Schema { column_defs, .. } = storage.get_schema(table_name)?;
            let key = storage.gen_id(&table_name)?;
            let row = Row::new(column_defs, columns, source)?;
            let row = storage.set_data(&key, row)?;

            Ok(Payload::Insert(row))
        }
        Statement::Update {
            table_name,
            selection,
            assignments,
        } => {
            let update = Update::new(assignments);
            let filter = Filter::new(storage, selection.as_ref(), None);
            let table_name = get_table_name(table_name)?;

            let columns = fetch_columns(storage, table_name)?;
            let num_rows = fetch(storage, table_name, &columns, filter)?
                .map(|item| {
                    let (columns, key, row) = item?;

                    Ok((key, update.apply(columns, row)?))
                })
                .try_fold::<_, _, Result<_>>(0, |num, item: Result<(T, Row)>| {
                    let (key, row) = item?;
                    storage.set_data(&key, row)?;

                    Ok(num + 1)
                })?;

            Ok(Payload::Update(num_rows))
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let filter = Filter::new(storage, selection.as_ref(), None);
            let table_name = get_table_name(table_name)?;

            let columns = fetch_columns(storage, table_name)?;
            let num_rows = fetch(storage, table_name, &columns, filter)?
                .try_fold::<_, _, Result<_>>(0, |num: usize, item| {
                    let (_, key, _) = item?;
                    storage.del_data(&key)?;

                    Ok(num + 1)
                })?;

            Ok(Payload::Update(num_rows))
        }
        _ => Err(ExecuteError::QueryNotSupported.into()),
    }
}
