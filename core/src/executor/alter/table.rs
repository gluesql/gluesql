use {
    super::{validate, AlterError},
    crate::{
        ast::{
            ColumnDef, ColumnOption, ColumnOptionDef, ObjectName, Query, SetExpr, TableFactor,
            Values,
        },
        data::{get_name, Schema, TableError},
        executor::{evaluate_stateless, select::select},
        prelude::{DataType, Value},
        result::{Error, IntoControlFlow, MutResult, Result, TrySelf},
        store::{GStore, GStoreMut},
    },
    futures::stream::{self, TryStreamExt},
    std::{
        iter,
        ops::ControlFlow::{Break, Continue},
    },
};

pub async fn create_table<T: GStore + GStoreMut>(
    storage: T,
    name: &ObjectName,
    column_defs: &[ColumnDef],
    if_not_exists: bool,
    source: &Option<Box<Query>>,
) -> MutResult<T, ()> {
    let (storage, target_table_name) = get_name(name).try_self(storage)?;
    let schema = (|| async {
        let target_columns_defs = match source.as_ref().map(AsRef::as_ref) {
            Some(Query { body, .. }) => match body {
                SetExpr::Select(select_query) => match &select_query.from.relation {
                    TableFactor::Table {
                        name: source_name, ..
                    } => {
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
                    TableFactor::Series { .. } => {
                        let column_def = ColumnDef {
                            name: "N".into(),
                            data_type: DataType::Int,
                            options: vec![ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }],
                        };

                        vec![column_def]
                    }
                    _ => {
                        return Err(Error::Table(TableError::Unreachable));
                    }
                },
                SetExpr::Values(Values(values_list)) => {
                    let first_len = values_list[0].len();
                    let init_types = iter::repeat(None)
                        .take(first_len)
                        .collect::<Vec<Option<DataType>>>();
                    let column_types =
                        values_list
                            .iter()
                            .try_fold(init_types, |column_types, exprs| {
                                let column_types = column_types
                                    .iter()
                                    .zip(exprs.iter())
                                    .map(|(column_type, expr)| match column_type {
                                        Some(data_type) => Ok(Some(data_type.to_owned())),
                                        None => evaluate_stateless(None, expr)
                                            .and_then(Value::try_from)
                                            .map(|value| value.get_type()),
                                    })
                                    .collect::<Result<Vec<Option<DataType>>>>()
                                    .into_control_flow()?;

                                match column_types.iter().any(Option::is_none) {
                                    true => Continue(column_types),
                                    false => Break(Ok(column_types)),
                                }
                            });
                    let column_types = match column_types {
                        Continue(column_types) => column_types,
                        Break(column_types) => column_types?,
                    };
                    let column_defs = column_types
                        .iter()
                        .map(|column_type| match column_type {
                            Some(column_type) => column_type.to_owned(),
                            None => DataType::Text,
                        })
                        .enumerate()
                        .map(|(i, data_type)| ColumnDef {
                            name: format!("column{}", i + 1),
                            data_type,
                            options: vec![ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Null,
                            }],
                        })
                        .collect::<Vec<_>>();

                    column_defs
                }
            },
            None => column_defs.to_vec(),
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

            storage.append_data(target_table_name, rows).await
        }
        None => Ok((storage, ())),
    }
}

pub async fn drop_table<T: GStore + GStoreMut>(
    storage: T,
    table_names: &[ObjectName],
    if_exists: bool,
) -> MutResult<T, ()> {
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
