use {
    super::{validate, validate_column_names, AlterError},
    crate::{
        ast::{ColumnDef, ForeignKey, Query, SetExpr, TableFactor, Values},
        data::{Schema, TableError},
        executor::{evaluate_stateless, select::select},
        prelude::{DataType, Value},
        result::{Error, Result},
        store::{GStore, GStoreMut},
    },
    futures::stream::TryStreamExt,
    serde::Serialize,
    std::fmt,
};

pub struct CreateTableOptions<'a> {
    pub target_table_name: &'a str,
    pub column_defs: Option<&'a [ColumnDef]>,
    pub if_not_exists: bool,
    pub source: &'a Option<Box<Query>>,
    pub engine: &'a Option<String>,
    pub foreign_keys: &'a Vec<ForeignKey>,
    pub comment: &'a Option<String>,
}

pub async fn create_table<T: GStore + GStoreMut>(
    storage: &mut T,
    CreateTableOptions {
        target_table_name,
        column_defs,
        if_not_exists,
        source,
        engine,
        foreign_keys,
        comment,
    }: CreateTableOptions<'_>,
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

    for foreign_key in foreign_keys {
        let ForeignKey {
            column,
            referred_table,
            referred_column,
            ..
        } = foreign_key;
        let foreign_schema =
            storage
                .fetch_schema(referred_table)
                .await?
                .ok_or_else(|| -> Error {
                    AlterError::ForeignTableNotFound(referred_table.to_owned()).into()
                })?;

        let foreign_column_def = foreign_schema
            .column_defs
            .and_then(|foreign_column_defs| {
                foreign_column_defs
                    .into_iter()
                    .find(|column_def| column_def.name == *referred_column)
            })
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
                referred_table: referred_table.to_owned(),
                referred_column: referred_column.to_owned(),
            }
            .into());
        }
    }

    if storage.fetch_schema(target_table_name).await?.is_none() {
        let schema = Schema {
            table_name: target_table_name.to_owned(),
            column_defs: target_columns_defs,
            indexes: vec![],
            engine: engine.clone(),
            foreign_keys: foreign_keys.clone(),
            comment: comment.clone(),
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
    cascade: bool,
) -> Result<()> {
    for table_name in table_names {
        let schema = storage.fetch_schema(table_name).await?;

        if !if_exists {
            schema.ok_or_else(|| AlterError::TableNotFound(table_name.to_owned()))?;
        }

        let schemas = storage.fetch_all_schemas().await?;
        let referring_children: Vec<ReferringChild> = schemas
            .into_iter()
            .flat_map(
                |Schema {
                     table_name: referring_table_name,
                     foreign_keys,
                     ..
                 }| {
                    foreign_keys
                        .into_iter()
                        .filter_map(
                            |ForeignKey {
                                 name: constraint_name,
                                 referred_table,
                                 ..
                             }| {
                                if &referred_table == table_name
                                    && &referring_table_name != table_name
                                {
                                    return Some(ReferringChild {
                                        table_name: referring_table_name.clone(),
                                        constraint_name,
                                    });
                                }

                                None
                            },
                        )
                        .collect::<Vec<_>>()
                },
            )
            .collect();

        if !referring_children.is_empty() && !cascade {
            return Err(AlterError::CannotDropTableParentOnReferringChildren {
                parent: table_name.into(),
                referring_children,
            }
            .into());
        }

        for ReferringChild {
            constraint_name, ..
        } in referring_children
        {
            let mut schema = storage
                .fetch_schema(table_name)
                .await?
                .ok_or_else(|| AlterError::TableNotFound(table_name.to_owned()))?;
            schema
                .foreign_keys
                .retain(|foreign_key| foreign_key.name != constraint_name);
            storage.insert_schema(&schema).await?;
        }

        storage.delete_schema(table_name).await?;
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct ReferringChild {
    pub table_name: String,
    pub constraint_name: String,
}

impl fmt::Display for ReferringChild {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "constraint {} on table {} depends on table parent",
            self.constraint_name, self.table_name
        )
    }
}
