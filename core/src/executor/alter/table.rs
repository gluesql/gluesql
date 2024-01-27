use std::fmt;

use serde::Serialize;

use crate::ast::ForeignKey;

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
    foreign_keys: &Option<Vec<ForeignKey>>,
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

    if let Some(foreign_keys) = foreign_keys.as_deref() {
        for foreign_key in foreign_keys {
            let ForeignKey {
                name,
                column,
                foreign_table,
                referred_column,
                on_delete,
                on_update,
            } = foreign_key;
            // 1. check if foreign_table exists
            // 2. check if referred_column exists in foreign_table
            // 3. check if column exists in target_table
            // 4. check if column and referred_column have same data type
            // 5. check if column and referred_column have same nullable
            // 6. check if column and referred_column have same unique
            // 7. check if on_delete and on_update are valid
            let foreign_schema =
                storage
                    .fetch_schema(foreign_table)
                    .await?
                    .ok_or_else(|| -> Error {
                        AlterError::ForeignTableNotFound(foreign_table.to_owned()).into()
                    })?;

            let foreign_column_def = foreign_schema
                .column_defs
                .unwrap()
                .into_iter()
                .find(|column_def| column_def.name == *referred_column)
                .ok_or_else(|| -> Error {
                    AlterError::ForeignKeyColumnNotFound(referred_column.to_owned()).into()
                })?;

            let target_column_def = target_columns_defs
                .as_deref()
                .and_then(|column_defs| {
                    column_defs
                        .iter()
                        .find(|column_def| column_def.name == *column)
                })
                .ok_or_else(|| -> Error {
                    AlterError::ForeignKeyColumnNotFound(column.to_owned()).into()
                })?;

            if target_column_def.data_type != foreign_column_def.data_type {
                return Err(AlterError::ForeignKeyDataTypeMismatch {
                    column: column.to_owned(),
                    column_type: target_column_def.data_type.to_owned(),
                    foreign_column: referred_column.to_owned(),
                    foreign_column_type: foreign_column_def.data_type.to_owned(),
                }
                .into());
            }

            if foreign_column_def.unique.is_none() {
                return Err(AlterError::ReferredColumnNotUnique {
                    foreign_table: foreign_table.to_owned(),
                    referred_column: referred_column.to_owned(),
                }
                .into());
            }

            // if on_delete.is_some() && on_delete != Some("cascade".to_owned()) {
            //     return Err(AlterError::ForeignKeyInvalidAction {
            //         action: on_delete.to_owned().unwrap(),
            //     }
            //     .into());
            // }

            // if on_update.is_some() && on_update != Some("cascade".to_owned()) {
            //     return Err(AlterError::ForeignKeyInvalidAction {
            //         action: on_update.to_owned().unwrap(),
            //     }
            //     .into());
            // }
        }
    }

    if storage.fetch_schema(target_table_name).await?.is_none() {
        let schema = Schema {
            table_name: target_table_name.to_owned(),
            column_defs: target_columns_defs,
            indexes: vec![],
            engine: engine.clone(),
            foreign_keys: foreign_keys.to_owned(),
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

        let schemas = storage.fetch_all_schemas().await?;
        let refferencing_children: Vec<RefferencingChild> = schemas
            .into_iter()
            .filter_map(
                |Schema {
                     table_name: refferencing_table_name,
                     foreign_keys,
                     ..
                 }| {
                    foreign_keys.map(|foreign_keys| {
                        foreign_keys
                            .into_iter()
                            .filter_map(
                                |ForeignKey {
                                     name,
                                     foreign_table,
                                     ..
                                 }| {
                                    if &foreign_table == table_name
                                        && &refferencing_table_name != table_name
                                    {
                                        return Some(RefferencingChild {
                                            table_name: refferencing_table_name.clone(),
                                            constraint_name: name
                                                .unwrap_or("defaultFK".to_owned())
                                                .to_owned(),
                                        });
                                    }

                                    None
                                },
                            )
                            .collect::<Vec<_>>()
                    })
                },
            )
            .flatten()
            .collect();

        if refferencing_children.len() > 0 {
            return Err(AlterError::CannotDropTableParentOnDependentChildren {
                parent_table_name: table_name.into(),
                children: refferencing_children,
            }
            .into());
        }

        storage.delete_schema(table_name).await?;
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct RefferencingChild {
    pub table_name: String,
    pub constraint_name: String,
}

impl fmt::Display for RefferencingChild {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "constraint {} on table {} depends on table parent",
            self.constraint_name, self.table_name
        )
    }
}
