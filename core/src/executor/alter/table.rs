use {
    super::{validate, validate_column_names, AlterError},
    crate::{
        ast::{ColumnDef, Query, SetExpr, Values},
        data::Schema,
        executor::{
            evaluate_stateless,
            select::{select, select_with_labels},
        },
        prelude::{DataType, Value},
        result::{IntoControlFlow, Result},
        store::{GStore, GStoreMut},
    },
    futures::stream::{StreamExt, TryStreamExt},
    std::{
        iter,
        ops::ControlFlow::{Break, Continue},
    },
};

pub async fn create_table<T: GStore + GStoreMut>(
    storage: &mut T,
    target_table_name: &str,
    column_defs: Option<&[ColumnDef]>,
    if_not_exists: bool,
    source: &Option<Box<Query>>,
    engine: &Option<String>,
) -> Result<()> {
    let target_columns_defs = match source.as_deref() {
        Some(Query {
            body,
            order_by,
            limit,
            offset,
        }) => match body {
            SetExpr::Select(select_query) => {
                let query = Query {
                    body: SetExpr::Select(select_query.clone()),
                    order_by: order_by.clone(),
                    limit: limit.clone(),
                    offset: offset.clone(),
                };

                let (labels, rows) = select_with_labels(storage, &query, None).await?;

                match labels {
                    Some(labels) => {
                        let rows = rows
                            .map(|row| row?.try_into_vec())
                            .try_collect::<Vec<_>>()
                            .await?;

                        let first_len = labels.len();
                        let init_types = iter::repeat(None)
                            .take(first_len)
                            .collect::<Vec<Option<DataType>>>();
                        let column_types =
                            rows.iter().try_fold(init_types, |column_types, values| {
                                let column_types = column_types
                                    .iter()
                                    .zip(values.iter())
                                    .map(|(column_type, value)| match column_type {
                                        Some(data_type) => Ok(Some(data_type.to_owned())),
                                        None => Ok(value.get_type()),
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
                            .zip(labels.into_iter())
                            .map(|(data_type, name)| ColumnDef {
                                name,
                                data_type,
                                nullable: true,
                                default: None,
                                unique: None,
                            })
                            .collect::<Vec<_>>();

                        Some(column_defs)
                    }
                    None => None,
                }
            }
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
                        nullable: true,
                        default: None,
                        unique: None,
                    })
                    .collect::<Vec<_>>();

                Some(column_defs)
            }
        },
        None if column_defs.is_some() => column_defs.map(<[ColumnDef]>::to_vec),
        None => None,
    };

    if let Some(column_defs) = target_columns_defs.as_deref() {
        validate_column_names(column_defs)?;

        for column_def in column_defs {
            validate(column_def)?;
        }
    }

    if storage.fetch_schema(target_table_name).await?.is_none() {
        let schema = Schema {
            table_name: target_table_name.to_owned(),
            column_defs: target_columns_defs,
            indexes: vec![],
            engine: engine.clone(),
        };

        storage.insert_schema(&schema).await?;
    } else if !if_not_exists {
        return Err(AlterError::TableAlreadyExists(target_table_name.to_owned()).into());
    }

    match source {
        Some(query) => {
            let rows = select(storage, query, None)
                .await?
                .map_ok(Into::into)
                .try_collect()
                .await?;

            storage
                .append_data(target_table_name, rows)
                .await
                .map(|_| ())
        }
        None => Ok(()),
    }
}

pub async fn drop_table<T: GStore + GStoreMut>(
    storage: &mut T,
    table_names: &[String],
    if_exists: bool,
) -> Result<()> {
    for table_name in table_names {
        let schema = storage.fetch_schema(table_name).await?;

        if !if_exists {
            schema.ok_or_else(|| AlterError::TableNotFound(table_name.to_owned()))?;
        }

        storage.delete_schema(table_name).await?;
    }

    Ok(())
}
