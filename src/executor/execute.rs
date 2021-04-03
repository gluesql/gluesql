#[cfg(feature = "alter-table")]
use super::alter::alter_table;
use {
    super::{
        alter::{create_table, drop},
        fetch::{fetch, fetch_columns},
        filter::Filter,
        select::select_with_labels,
        update::Update,
        validate::{validate_unique, ColumnValidation},
    },
    crate::{
        data::{bulk_build_rows_expr, bulk_build_rows_row, get_name, Row, Schema},
        parse_sql::Query,
        result::{MutResult, Result},
        store::{AlterTable, AutoIncrement, Store, StoreMut},
    },
    futures::stream::TryStreamExt,
    serde::Serialize,
    sqlparser::ast::{Query as AstQuery, SetExpr, Statement, Values},
    std::{fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[cfg(feature = "auto-increment")]
use super::column_options::auto_increment;

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum ExecuteError {
    #[error("query not supported")]
    QueryNotSupported,

    #[error("unsupported insert value type: {0}")]
    UnreachableUnsupportedInsertValueType(String),

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

pub async fn execute<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable + AutoIncrement>(
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
    macro_rules! try_into {
        ($storage: expr, $expr: expr) => {
            match $expr {
                Err(e) => {
                    return Err(($storage, e));
                }
                Ok(v) => v,
            }
        };
    }

    macro_rules! try_into {
        ($self: expr, $expr: expr) => {
            match $expr {
                Err(e) => {
                    return Err(($self, e));
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
        } => create_table(storage, name, columns, *if_not_exists)
            .await
            .map(|(storage, _)| (storage, Payload::Create)),
        Statement::Drop {
            object_type,
            names,
            if_exists,
            ..
        } => drop(storage, object_type, names, *if_exists)
            .await
            .map(|(storage, _)| (storage, Payload::DropTable)),
        #[cfg(feature = "alter-table")]
        Statement::AlterTable { name, operation } => alter_table(storage, name, operation)
            .await
            .map(|(storage, _)| (storage, Payload::AlterTable)),

        //-- Rows
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => {
            #[allow(unused_variables)]
            let (rows, column_defs, column_validation, table_name) = try_block!(storage, {
                let table_name = get_name(table_name)?;
                let Schema { column_defs, .. } = storage
                    .fetch_schema(table_name)
                    .await?
                    .ok_or(ExecuteError::TableNotExists)?;
                let column_defs = Rc::from(column_defs);
                let column_validation = ColumnValidation::All(Rc::clone(&column_defs));

                let rows = match &source.body {
                    SetExpr::Values(Values(values_list)) => {
                        bulk_build_rows_expr(
                            &storage,
                            &column_defs,
                            columns,
                            values_list.to_vec(),
                            true,
                            true,
                        )
                        .await?
                    }
                    SetExpr::Select(select_query) => {
                        let query = Box::new(AstQuery {
                            with: None,
                            body: SetExpr::Select(select_query.clone()),
                            order_by: vec![],
                            limit: None,
                            offset: None,
                            fetch: None,
                        });
                        let rows = select(&storage, &query).await?.1;
                        bulk_build_rows_row(&storage, &column_defs, columns, rows, true, true)
                            .await?
                        // TODO: Improve efficiency, this should happen much earlier than this.
                    }
                    set_expr => {
                        return Err(ExecuteError::UnreachableUnsupportedInsertValueType(
                            set_expr.to_string(),
                        )
                        .into());
                    }
                };

                Ok((rows, column_defs, column_validation, table_name))
            });

            #[cfg(feature = "auto-increment")]
            let (storage, rows) =
                auto_increment::run(storage, rows, &column_defs, table_name).await?;

            try_into!(storage, {
                validate_unique(&storage, &table_name, column_validation, rows.iter()).await
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

                let column_validation =
                    ColumnValidation::SpecifiedColumns(Rc::from(column_defs), columns_to_update);
                validate_unique(
                    &storage,
                    &table_name,
                    column_validation,
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
            let (labels, rows) = try_into!(storage, { select(&storage, query).await });

            Ok((storage, Payload::Select { labels, rows }))
        }
        _ => Err((storage, ExecuteError::QueryNotSupported.into())),
    }
}

async fn select<T: 'static + Debug>(
    storage: &dyn Store<T>,
    query: &AstQuery,
) -> Result<(Vec<String>, Vec<Row>)> {
    let (labels, rows) = select_with_labels(storage, query, None, true).await?;
    let rows = rows.try_collect::<Vec<_>>().await?;

    Ok((labels, rows))
}
