use {
    super::{validate, AlterError},
    crate::{
        ast::{ColumnDef, ObjectName, Query, SetExpr, TableFactor},
        data::{get_name, Schema},
        executor::select::select,
        result::{Error, MutResult, TrySelf},
        store::{GStore, GStoreMut},
    },
    futures::stream::{self, TryStreamExt},
};

pub async fn create_table<T, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    name: &ObjectName,
    column_defs: &[ColumnDef],
    if_not_exists: bool,
    source: &Option<Box<Query>>,
) -> MutResult<U, ()> {
    let (storage, target_table_name) = get_name(name).try_self(storage)?;
    let schema = (|| async {
        let target_columns_defs = match source.as_ref().map(AsRef::as_ref) {
            Some(Query {
                body: SetExpr::Select(select_query),
                ..
            }) => {
                let TableFactor::Table {
                    name: source_name, ..
                } = &select_query.from.relation;
                let table_name = get_name(source_name)?;
                let schema = storage.fetch_schema(table_name).await?;
                let Schema {
                    column_defs: source_column_defs,
                    ..
                } = schema.ok_or_else(|| -> Error {
                    AlterError::CtasSourceTableNotFound(table_name.to_owned()).into()
                })?;
                source_column_defs
            }
            _ => column_defs.to_vec(),
        };

        let schema = Schema {
            table_name: target_table_name.to_string(),
            column_defs: target_columns_defs,
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

    let storage = match schema.try_self(storage)? {
        (storage, Some(schema)) => storage.insert_schema(&schema).await?.0,
        (storage, None) => storage,
    };

    match source {
        Some(q) => {
            let (storage, rows) = async { select(&storage, q, None).await?.try_collect().await }
                .await
                .try_self(storage)?;

            storage.insert_data(target_table_name, rows).await
        }
        None => Ok((storage, ())),
    }
}

pub async fn drop_table<T, U: GStore<T> + GStoreMut<T>>(
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
