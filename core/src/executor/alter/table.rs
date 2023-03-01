use {
    super::{validate, validate_column_names, AlterError},
    crate::{
        ast::{ColumnDef, Query, SetExpr, TableFactor, Values},
        data::{Schema, TableError},
        executor::{evaluate_stateless, select::select},
        prelude::{DataType, Value},
        result::{Error, IntoControlFlow, Result},
        store::{GStore, GStoreMut},
    },
    chrono::Utc,
    futures::stream::TryStreamExt,
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
        Some(Query { body, .. }) => match body {
            SetExpr::Select(select_query) => match &select_query.from.relation {
                TableFactor::Table { name, .. } => {
                    let schema = storage.fetch_schema(name).await?;
                    let Some(Schema {
                        column_defs: source_column_defs,
                        ..
                    }) = schema else {
                        return Err(AlterError::CtasSourceTableNotFound(name.to_owned()).into());
                    };

                    source_column_defs
                }
                TableFactor::Series { .. } => {
                    let column_def = ColumnDef {
                        name: "N".into(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        unique: None,
                    };

                    Some(vec![column_def])
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
            created: Utc::now().naive_utc(),
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
        let Some(_) = storage.fetch_schema(table_name).await? else {
            return Err(AlterError::TableNotFound(table_name.to_owned()).into());
        };

        if !if_exists {
            return Err(AlterError::TableNotFound(table_name.to_owned()).into());
        }

        storage.delete_schema(table_name).await?;
    }

    Ok(())
}
