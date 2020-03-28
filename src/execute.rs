use nom_sql::{Column, DeleteStatement, InsertStatement, SqlQuery, UpdateStatement};
use std::fmt::Debug;
use thiserror::Error;

use crate::data::Row;
use crate::executor::{fetch, fetch_columns, fetch_select_params, select, Filter, Update};
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
    sql_query: &SqlQuery,
) -> Result<Payload> {
    match sql_query {
        SqlQuery::CreateTable(statement) => {
            storage.set_schema(statement)?;

            Ok(Payload::Create)
        }
        SqlQuery::Select(statement) => {
            let params = fetch_select_params(storage, statement)?;
            let rows = select(storage, statement, &params, None)?.collect::<Result<_>>()?;

            Ok(Payload::Select(rows))
        }
        SqlQuery::Insert(statement) => {
            let (table_name, insert_fields, insert_data) = match statement {
                InsertStatement {
                    table,
                    fields,
                    data,
                    ..
                } => (&table.name, fields, data),
            };
            let create_fields = storage.get_schema(table_name)?.fields;
            let key = storage.gen_id(table_name)?;
            let row = Row::new(create_fields, insert_fields, insert_data)?;

            let row = storage.set_data(&key, row)?;

            Ok(Payload::Insert(row))
        }
        SqlQuery::Delete(statement) => {
            let DeleteStatement {
                table,
                where_clause,
            } = statement;
            let filter = Filter::new(storage, where_clause.as_ref(), None);

            let columns = fetch_columns(storage, table)?;
            let num_rows = fetch(storage, table, &columns, filter)?.try_fold::<_, _, Result<_>>(
                0,
                |num: usize, item: Result<(&Vec<Column>, T, Row)>| {
                    let (_, key, _) = item?;
                    storage.del_data(&key)?;

                    Ok(num + 1)
                },
            )?;

            Ok(Payload::Delete(num_rows))
        }
        SqlQuery::Update(statement) => {
            let UpdateStatement {
                table,
                fields,
                where_clause,
            } = statement;
            let update = Update::new(fields);
            let filter = Filter::new(storage, where_clause.as_ref(), None);
            let columns = fetch_columns(storage, table)?;

            let num_rows = fetch(storage, table, &columns, filter)?
                .map(|item| {
                    let (columns, key, row) = item?;

                    Ok((key, update.apply(columns, row)))
                })
                .try_fold::<_, _, Result<_>>(0, |num, item: Result<(T, Row)>| {
                    let (key, row) = item?;
                    storage.set_data(&key, row)?;

                    Ok(num + 1)
                })?;

            Ok(Payload::Update(num_rows))
        }
        _ => Err(ExecuteError::QueryNotSupported.into()),
    }
}
