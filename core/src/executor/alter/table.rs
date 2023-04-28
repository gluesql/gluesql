use {
    super::{validate, validate_column_names, AlterError},
    crate::{
        ast::{ColumnDef, Query},
        data::Schema,
        executor::{
            insert::{fetch_insert_rows, insert},
            select::select_with_labels,
        },
        prelude::DataType,
        result::Result,
        store::{GStore, GStoreMut},
    },
    futures::stream::StreamExt,
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
        Some(query) => {
            let (labels, mut rows) = select_with_labels(storage, &query, None).await?;

            match labels {
                Some(labels) => {
                    let first_len = labels.len();
                    let mut column_types = vec![None; first_len];

                    while let Some(row) = rows.next().await {
                        let row = row?;
                        for (i, (_, value)) in row.iter().enumerate() {
                            if column_types[i].is_some() {
                                continue;
                            }

                            column_types[i] = value.get_type();
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
        None if column_defs.is_some() => column_defs.map(<[ColumnDef]>::to_vec),
        None => None,
    };

    if let Some(column_defs) = target_columns_defs.as_deref() {
        validate_column_names(column_defs)?;

        for column_def in column_defs {
            validate(column_def).await?;
        }
    }

    let rows = match source.as_deref() {
        Some(_) => {
            let columns = target_columns_defs
                .clone()
                .map(|column_defs| {
                    column_defs
                        .into_iter()
                        .map(|column_def| column_def.name)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let rows = fetch_insert_rows(
                storage,
                None,
                &columns,
                source.as_deref().unwrap(),
                target_columns_defs.clone(),
            )
            .await?;

            Some(rows)
        }
        None => None,
    };

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

    match (source, rows) {
        (Some(_), Some(rows)) => insert(storage, target_table_name, rows).await.map(|_| ()),
        _ => Ok(()),
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
