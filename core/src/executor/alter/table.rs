use {
    super::{validate, validate_column_names, AlterError},
    crate::{
        ast::{ColumnDef, Query, SetExpr, TableFactor, Values},
        data::{Schema, TableError},
        executor::{evaluate_stateless, select::select},
        prelude::{DataType, Value},
        result::{Error, Result},
        store::{GStore, GStoreMut},
    },
    futures::stream::TryStreamExt,
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
                    let Schema {
                        column_defs: source_column_defs,
                        ..
                    } = schema.ok_or_else(|| -> Error {
                        AlterError::CtasSourceTableNotFound(name.to_owned()).into()
                    })?;

                    source_column_defs
                }
                TableFactor::Series { .. } => {
                    let column_def = ColumnDef {
                        name: "N".into(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        unique: None,
                        comment: None,
                    };

                    Some(vec![column_def])
                }
                _ => {
                    return Err(Error::Table(TableError::Unreachable));
                }
            },
            SetExpr::Values(Values(values_list)) => {
                let first_len = values_list[0].len();
                let mut column_types = vec![None; first_len];

                for exprs in values_list {
                    for (i, expr) in exprs.iter().enumerate() {
                        if column_types[i].is_some() {
                            continue;
                        }

                        column_types[i] = evaluate_stateless(None, expr)
                            .await
                            .and_then(Value::try_from)
                            .map(|value| value.get_type())?;
                    }

                    if column_types.iter().all(Option::is_some) {
                        break;
                    }
                }

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
                        comment: None,
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
            validate(column_def).await?;
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
