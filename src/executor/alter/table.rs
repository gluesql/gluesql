use crate::{
    executor::{execute, select::select},
    FetchError, TableError,
};

use {
    super::{validate, AlterError},
    crate::{
        ast::{ColumnDef, ObjectName, Query, SetExpr, Statement, TableFactor},
        data::{get_name, Schema},
        result::{MutResult, TrySelf},
        store::{GStore, GStoreMut},
    },
    futures::stream::{self, TryStreamExt},
    std::fmt::Debug,
};

pub async fn create_table<T: Debug, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    name: &ObjectName,
    column_defs: &[ColumnDef],
    if_not_exists: bool,
    source: &Option<Box<Query>>,
) -> MutResult<U, ()> {
    /*
    createtable = ||
    match (source, column_defs.is_empty) {
        (Some(q), true) => {
            fetchSchema with valitdation (source table exists)
            create table with type validation
            insert
        },
        (None, false) => {
            create table
        }
    }
    */
    let (target_columns, source_query) = match source.as_ref().map(AsRef::as_ref) {
        Some(Query {
            body: SetExpr::Select(select_query),
            ..
        }) => {
            let TableFactor::Table {
                name: source_name, ..
            } = &select_query.from.relation;
            let table_name = get_name(&source_name).unwrap();
            if let Some(Schema {
                column_defs: source_column_defs,
                ..
            }) = storage.fetch_schema(table_name).await.unwrap()
            {
                (source_column_defs, Some(select_query))
            } else {
                (column_defs.to_vec(), None) // unreachable?
            }
        }
        _ => (column_defs.to_vec(), None),
    };

    let schema = (|| async {
        let schema = Schema {
            table_name: get_name(name)?.to_string(),
            column_defs: target_columns,
            indexes: vec![],
        };

        for column_def in &schema.column_defs {
            validate(column_def)?;
        }

        match (
            storage.fetch_schema(&schema.table_name).await?,
            if_not_exists,
        ) {
            (None, _) => Ok(Some(schema)),
            (Some(_), true) => Ok(None),
            (Some(_), false) => {
                Err(AlterError::TableAlreadyExists(schema.table_name.to_owned()).into())
            }
        }
    })()
    .await;

    let schema = match schema {
        Ok(s) => s,
        Err(e) => {
            return Err((storage, e));
        }
    };

    let query = Query {
        body: SetExpr::Select(source_query.unwrap().to_owned()),
        order_by: vec![],
        limit: None,
        offset: None,
    };

    let (storage, rows) = (|| async {
        select(&storage, &query, None)
            .await?
            .try_collect::<Vec<_>>()
            .await
    })()
    .await
    .try_self(storage)?;

    if let Some(schema) = schema {
        let (storage, _) = storage.insert_schema(&schema).await?;
        return storage.insert_data(get_name(name).unwrap(), rows).await;
    } else {
        return Ok((storage, ()));
    }
}

pub async fn drop_table<T: Debug, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    table_names: &[ObjectName],
    if_exists: bool,
) -> MutResult<U, ()> {
    stream::iter(table_names.iter().map(Ok))
        .try_fold((storage, ()), |(storage, _), table_name| async move {
            let schema = (|| async {
                let table_name = get_name(table_name)?;
                let schema = storage.fetch_schema(table_name).await?;

                if !if_exists {
                    schema.ok_or_else(|| AlterError::TableNotFound(table_name.to_owned()))?;
                }

                Ok(table_name)
            })()
            .await;

            let schema = match schema {
                Ok(s) => s,
                Err(e) => {
                    return Err((storage, e));
                }
            };

            storage.delete_schema(schema).await
        })
        .await
}
