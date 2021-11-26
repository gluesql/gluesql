use crate::executor::{execute, select::select};

use {
    super::{validate, AlterError},
    crate::{
        ast::{ColumnDef, ObjectName, Query, SetExpr, Statement, TableFactor},
        data::{get_name, Schema},
        result::MutResult,
        store::{GStore, GStoreMut},
    },
    futures::stream::{self, TryStreamExt},
    std::{fmt::Debug, rc::Rc},
};

pub async fn create_table<T: Debug, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    name: &ObjectName,
    column_defs: &[ColumnDef],
    if_not_exists: bool,
    source: &Option<Box<Query>>,
) -> MutResult<U, ()> {
    // macro_rules! try_block {
    //     ($storage: expr, $block: block) => {{
    //         match (|| async { $block })().await {
    //             Err(e) => {
    //                 return Err(($storage, e));
    //             }
    //             Ok(v) => v,
    //         }
    //     }};
    // } // can't use try_block here?
    match source {
        Some(v) => {
            if let SetExpr::Select(select_query) = &v.body {
                let TableFactor::Table {
                    name: source_name, ..
                } = &select_query.from.relation;
                let table_name = get_name(&source_name).unwrap();
                if let Some(Schema {
                    column_defs: source_column_defs,
                    ..
                }) = storage.fetch_schema(table_name).await.unwrap()
                {
                    let schema = (|| async {
                        let schema = Schema {
                            table_name: get_name(name).unwrap().to_string(),
                            column_defs: source_column_defs.clone(),
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
                                Err(AlterError::TableAlreadyExists(schema.table_name.to_owned())
                                    .into())
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
                    let query = || Query {
                        body: SetExpr::Select(select_query.clone()),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    };

                    let rows = select(&storage, &query(), None)
                        .await
                        .unwrap()
                        .and_then(|row| {
                            // let column_defs = Rc::clone(&column_defs);
                            async move {
                                // row.validate(&source_column_defs)?; // should we validate again?

                                Ok(row)
                            }
                        })
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();

                    // let num_rows = rows.len();
                    // .map(|(storage, _)| (storage, Payload::Insert(num_rows))); // need to impl payload later

                    if let Some(schema) = schema {
                        let (storage, _) = storage.insert_schema(&schema).await?;
                        return storage.insert_data(get_name(name).unwrap(), rows).await;
                    } else {
                        return Ok((storage, ()));
                    }
                }
            }
            Ok((storage, ()))
        }
        None => {
            let schema = (|| async {
                let schema = Schema {
                    table_name: get_name(name)?.to_string(),
                    column_defs: column_defs.to_vec(),
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

            if let Some(schema) = schema {
                storage.insert_schema(&schema).await
            } else {
                Ok((storage, ()))
            }
        }
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
