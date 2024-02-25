use {
    super::{context::RowContext, evaluate::evaluate_stateless, filter::check_expr},
    crate::{
        ast::{
            ToSql,
            {
                ColumnDef, ColumnUniqueOption, Dictionary, Expr, IndexItem, Join, Query, Select,
                SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins, ToSqlUnquoted,
                Values,
            },
        },
        data::{get_alias, get_index, Key, Row, Value},
        executor::{evaluate::evaluate, select::select},
        result::Result,
        store::{DataRow, GStore},
    },
    async_recursion::async_recursion,
    futures::{
        future,
        stream::{self, Stream, StreamExt, TryStreamExt},
    },
    serde::Serialize,
    std::{borrow::Cow, collections::HashMap, fmt::Debug, iter, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum FetchError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table alias not found: {0}")]
    TableAliasNotFound(String),

    #[error("SERIES has wrong size: {0}")]
    SeriesSizeWrong(i64),

    #[error("table '{0}' has {1} columns available but {2} column aliases specified")]
    TooManyColumnAliases(String, usize, usize),
}

pub async fn fetch<'a, T: GStore>(
    storage: &'a T,
    table_name: &'a str,
    columns: Option<Rc<[String]>>,
    where_clause: Option<&'a Expr>,
) -> Result<impl Stream<Item = Result<(Key, Row)>> + 'a> {
    let columns = columns.unwrap_or_else(|| Rc::from([]));
    let rows = storage
        .scan_data(table_name)
        .await?
        .try_filter_map(move |(key, data_row)| {
            let row = match data_row {
                DataRow::Vec(values) => Row::Vec {
                    columns: Rc::clone(&columns),
                    values,
                },
                DataRow::Map(values) => Row::Map(values),
            };

            async move {
                let expr = match where_clause {
                    None => {
                        return Ok(Some((key, row)));
                    }
                    Some(expr) => expr,
                };

                let context = RowContext::new(table_name, Cow::Borrowed(&row), None);

                check_expr(storage, Some(Rc::new(context)), None, expr)
                    .await
                    .map(|pass| pass.then_some((key, row)))
            }
        });

    Ok(rows)
}

#[derive(futures_enum::Stream)]
pub enum Rows<I1, I2, I3, I4> {
    Derived(I1),
    Table(I2),
    Series(I3),
    Dictionary(I4),
}

pub async fn fetch_relation_rows<'a, T: GStore>(
    storage: &'a T,
    table_factor: &'a TableFactor,
    filter_context: &Option<Rc<RowContext<'a>>>,
) -> Result<impl Stream<Item = Result<Row>> + 'a> {
    let columns = Rc::from(
        fetch_relation_columns(storage, table_factor)
            .await?
            .unwrap_or_default(),
    );

    match table_factor {
        TableFactor::Derived { subquery, .. } => {
            let filter_context = filter_context.as_ref().map(Rc::clone);
            let rows =
                select(storage, subquery, filter_context)
                    .await?
                    .map_ok(move |row| match row {
                        Row::Vec { values, .. } => Row::Vec {
                            columns: Rc::clone(&columns),
                            values,
                        },
                        Row::Map(values) => Row::Map(values),
                    });

            Ok(Rows::Derived(rows))
        }
        TableFactor::Table { name, .. } => {
            let rows = {
                #[derive(futures_enum::Stream)]
                enum Rows<I1, I2, I3, I4> {
                    Indexed(I1),
                    PrimaryKey(I2),
                    PrimaryKeyEmpty(I3),
                    FullScan(I4),
                }

                match get_index(table_factor) {
                    Some(IndexItem::NonClustered {
                        name: index_name,
                        asc,
                        cmp_expr,
                    }) => {
                        let cmp_value = match cmp_expr {
                            Some((op, expr)) => {
                                let evaluated = evaluate(storage, None, None, expr).await?;

                                Some((op, evaluated.try_into()?))
                            }
                            None => None,
                        };

                        let rows = storage
                            .scan_indexed_data(name, index_name, *asc, cmp_value)
                            .await?
                            .map_ok(move |(_, data_row)| match data_row {
                                DataRow::Vec(values) => Row::Vec {
                                    columns: Rc::clone(&columns),
                                    values,
                                },
                                DataRow::Map(values) => Row::Map(values),
                            });

                        Rows::Indexed(rows)
                    }
                    Some(IndexItem::PrimaryKey(expr)) => {
                        let filter_context = filter_context.as_ref().map(Rc::clone);
                        let key = evaluate(storage, filter_context, None, expr)
                            .await
                            .and_then(Value::try_from)
                            .and_then(Key::try_from)?;

                        match storage.fetch_data(name, &key).await? {
                            Some(data_row) => {
                                let row = match data_row {
                                    DataRow::Vec(values) => Row::Vec {
                                        columns: Rc::clone(&columns),
                                        values,
                                    },
                                    DataRow::Map(values) => Row::Map(values),
                                };

                                Rows::PrimaryKey(stream::once(future::ready(Ok(row))))
                            }
                            None => Rows::PrimaryKeyEmpty(stream::empty()),
                        }
                    }
                    _ => {
                        let rows = storage.scan_data(name).await?.map_ok(move |(_, data_row)| {
                            match data_row {
                                DataRow::Vec(values) => Row::Vec {
                                    columns: Rc::clone(&columns),
                                    values,
                                },
                                DataRow::Map(values) => Row::Map(values),
                            }
                        });

                        Rows::FullScan(rows)
                    }
                }
            };

            Ok(Rows::Table(rows))
        }
        TableFactor::Series { size, .. } => {
            let value: Value = evaluate_stateless(None, size).await?.try_into()?;
            let size: i64 = value.try_into()?;
            let size = match size {
                n if n >= 0 => size,
                n => return Err(FetchError::SeriesSizeWrong(n).into()),
            };

            let columns = Rc::from(vec!["N".to_owned()]);
            let rows = (1..=size).map(move |v| {
                Ok(Row::Vec {
                    columns: Rc::clone(&columns),
                    values: vec![Value::I64(v)],
                })
            });

            Ok(Rows::Series(stream::iter(rows)))
        }
        TableFactor::Dictionary { dict, .. } => {
            let rows = {
                #[derive(futures_enum::Stream)]
                enum Rows<I1, I2, I3, I4> {
                    Tables(I1),
                    TableColumns(I2),
                    Indexes(I3),
                    Objects(I4),
                }

                match dict {
                    Dictionary::GlueObjects => {
                        let schemas = storage.fetch_all_schemas().await?;
                        let table_metas = storage
                            .scan_table_meta()
                            .await?
                            .collect::<Result<HashMap<_, _>>>()?;
                        let rows = schemas.into_iter().flat_map(move |schema| {
                            let meta = table_metas
                                .iter()
                                .find_map(|(table_name, hash_map)| {
                                    (table_name == &schema.table_name).then(|| hash_map.clone())
                                })
                                .unwrap_or_default();

                            let table_rows = HashMap::from([
                                ("OBJECT_NAME".to_owned(), Value::Str(schema.table_name)),
                                ("OBJECT_TYPE".to_owned(), Value::Str("TABLE".to_owned())),
                            ])
                            .into_iter()
                            .chain(meta)
                            .collect::<HashMap<_, _>>();

                            let index_rows = schema.indexes.into_iter().map(|index| {
                                HashMap::from([
                                    ("OBJECT_NAME".to_owned(), Value::Str(index.name)),
                                    ("OBJECT_TYPE".to_owned(), Value::Str("INDEX".to_owned())),
                                ])
                            });

                            iter::once(table_rows)
                                .chain(index_rows)
                                .map(|hash_map| Ok(Row::Map(hash_map)))
                        });

                        Rows::Objects(stream::iter(rows))
                    }
                    Dictionary::GlueTables => {
                        let schemas = storage.fetch_all_schemas().await?;
                        let rows = schemas.into_iter().map(move |schema| {
                            Ok(Row::Vec {
                                columns: Rc::clone(&columns),
                                values: vec![Value::Str(schema.table_name)],
                            })
                        });

                        Rows::Tables(stream::iter(rows))
                    }
                    Dictionary::GlueTableColumns => {
                        let schemas = storage.fetch_all_schemas().await?;
                        let rows = schemas.into_iter().flat_map(move |schema| {
                            let columns = Rc::clone(&columns);
                            let table_name = schema.table_name;

                            schema
                                .column_defs
                                .unwrap_or_default()
                                .into_iter()
                                .enumerate()
                                .map(move |(index, column_def)| {
                                    let values = vec![
                                        Value::Str(table_name.clone()),
                                        Value::Str(column_def.name),
                                        Value::I64(index as i64 + 1),
                                        Value::Bool(column_def.nullable),
                                        Value::Str(
                                            column_def
                                                .unique
                                                .map(|unique| unique.to_sql())
                                                .unwrap_or_default(),
                                        ),
                                        Value::Str(
                                            column_def
                                                .default
                                                .map(|expr| expr.to_sql())
                                                .unwrap_or_default(),
                                        ),
                                    ];

                                    Ok(Row::Vec {
                                        columns: Rc::clone(&columns),
                                        values,
                                    })
                                })
                        });

                        Rows::TableColumns(stream::iter(rows))
                    }
                    Dictionary::GlueIndexes => {
                        let schemas = storage.fetch_all_schemas().await?;
                        let rows = schemas.into_iter().flat_map(move |schema| {
                            let column_defs = schema.column_defs.unwrap_or_default();
                            let primary_column = column_defs.iter().find_map(|column_def| {
                                let ColumnDef { name, unique, .. } = column_def;

                                (unique == &Some(ColumnUniqueOption { is_primary: true }))
                                    .then_some(name)
                            });

                            let clustered = match primary_column {
                                Some(column_name) => {
                                    let values = vec![
                                        Value::Str(schema.table_name.clone()),
                                        Value::Str("PRIMARY".to_owned()),
                                        Value::Str("BOTH".to_owned()),
                                        Value::Str(column_name.to_owned()),
                                        Value::Bool(true),
                                    ];

                                    let row = Row::Vec {
                                        columns: Rc::clone(&columns),
                                        values,
                                    };

                                    vec![Ok(row)]
                                }
                                None => Vec::new(),
                            };

                            let columns = Rc::clone(&columns);
                            let non_clustered = schema.indexes.into_iter().map(move |index| {
                                let values = vec![
                                    Value::Str(schema.table_name.clone()),
                                    Value::Str(index.name),
                                    Value::Str(index.order.to_string()),
                                    Value::Str(index.expr.to_sql_unquoted()),
                                    Value::Bool(false),
                                ];

                                Ok(Row::Vec {
                                    columns: Rc::clone(&columns),
                                    values,
                                })
                            });

                            clustered.into_iter().chain(non_clustered)
                        });

                        Rows::Indexes(stream::iter(rows))
                    }
                }
            };

            Ok(Rows::Dictionary(rows))
        }
    }
}

pub async fn fetch_columns<T: GStore>(
    storage: &T,
    table_name: &str,
) -> Result<Option<Vec<String>>> {
    let columns = storage
        .fetch_schema(table_name)
        .await?
        .ok_or_else(|| FetchError::TableNotFound(table_name.to_owned()))?
        .column_defs
        .map(|column_defs| {
            column_defs
                .into_iter()
                .map(|column_def| column_def.name)
                .collect()
        });

    Ok(columns)
}

#[async_recursion(?Send)]
pub async fn fetch_relation_columns<T: GStore>(
    storage: &T,
    table_factor: &TableFactor,
) -> Result<Option<Vec<String>>> {
    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            let columns = fetch_columns(storage, name).await?;
            match (columns, alias) {
                (columns, None) => Ok(columns),
                (None, Some(_)) => Ok(None),
                (Some(columns), Some(alias)) if alias.columns.len() > columns.len() => {
                    Err(FetchError::TooManyColumnAliases(
                        name.to_string(),
                        columns.len(),
                        alias.columns.len(),
                    )
                    .into())
                }
                (Some(columns), Some(alias)) => Ok(Some(
                    alias
                        .columns
                        .iter()
                        .cloned()
                        .chain(columns[alias.columns.len()..columns.len()].to_vec())
                        .collect(),
                )),
            }
        }
        TableFactor::Series { .. } => Ok(Some(vec!["N".to_owned()])),
        TableFactor::Dictionary { dict, .. } => Ok(Some(match dict {
            Dictionary::GlueObjects => vec![
                "OBJECT_NAME".to_owned(),
                "OBJECT_TYPE".to_owned(),
                "CREATED".to_owned(),
            ],
            Dictionary::GlueTables => vec!["TABLE_NAME".to_owned()],
            Dictionary::GlueTableColumns => vec![
                "TABLE_NAME".to_owned(),
                "COLUMN_NAME".to_owned(),
                "COLUMN_ID".to_owned(),
                "NULLABLE".to_owned(),
                "KEY".to_owned(),
                "DEFAULT".to_owned(),
            ],
            Dictionary::GlueIndexes => vec![
                "TABLE_NAME".to_owned(),
                "INDEX_NAME".to_owned(),
                "ORDER".to_owned(),
                "EXPRESSION".to_owned(),
                "UNIQUENESS".to_owned(),
            ],
        })),
        TableFactor::Derived {
            subquery: Query { body, .. },
            alias:
                TableAlias {
                    columns: alias_columns,
                    name,
                },
        } => match body {
            SetExpr::Select(statement) => {
                let Select {
                    from:
                        TableWithJoins {
                            relation, joins, ..
                        },
                    projection,
                    ..
                } = statement.as_ref();

                let labels = fetch_labels(storage, relation, joins, projection).await?;
                match labels {
                    None => Ok(None),
                    Some(labels) if alias_columns.is_empty() => Ok(Some(labels)),
                    Some(labels) if alias_columns.len() > labels.len() => {
                        Err(FetchError::TooManyColumnAliases(
                            name.to_string(),
                            labels.len(),
                            alias_columns.len(),
                        )
                        .into())
                    }
                    Some(labels) => Ok(Some(
                        alias_columns
                            .iter()
                            .cloned()
                            .chain(labels[alias_columns.len()..labels.len()].to_vec())
                            .collect(),
                    )),
                }
            }
            SetExpr::Values(Values(values_list)) => {
                let total_len = values_list[0].len();
                let alias_len = alias_columns.len();
                if alias_len > total_len {
                    return Err(FetchError::TooManyColumnAliases(
                        name.into(),
                        total_len,
                        alias_len,
                    )
                    .into());
                }
                let labels = (alias_len + 1..=total_len).map(|i| format!("column{}", i));
                let labels = alias_columns
                    .iter()
                    .cloned()
                    .chain(labels)
                    .collect::<Vec<_>>();

                Ok(Some(labels))
            }
        },
    }
}

async fn fetch_join_columns<'a, T: GStore>(
    storage: &T,
    joins: &'a [Join],
) -> Result<Option<Vec<(&'a String, Vec<String>)>>> {
    let columns = stream::iter(joins)
        .filter_map(|join| async {
            let relation = &join.relation;
            let alias = get_alias(relation);

            fetch_relation_columns(storage, relation)
                .await
                .map(|columns| Some((alias, columns?)))
                .transpose()
        })
        .try_collect::<Vec<_>>()
        .await?;

    Ok((columns.len() == joins.len()).then_some(columns))
}

pub async fn fetch_labels<T: GStore>(
    storage: &T,
    relation: &TableFactor,
    joins: &[Join],
    projection: &[SelectItem],
) -> Result<Option<Vec<String>>> {
    let table_alias = get_alias(relation);
    let columns = fetch_relation_columns(storage, relation).await?;
    let join_columns = fetch_join_columns(storage, joins).await?;

    if (columns.is_none() || join_columns.is_none())
        && projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
            )
        })
    {
        return Ok(None);
    }

    let columns = columns.unwrap_or_default();
    let join_columns = join_columns.unwrap_or_default();

    projection
        .iter()
        .flat_map(|item| match item {
            SelectItem::Wildcard => {
                let columns = columns.iter().cloned();
                let join_columns = join_columns.iter().flat_map(|(_, columns)| columns.clone());

                columns.chain(join_columns).map(Ok).collect()
            }
            SelectItem::QualifiedWildcard(target_table_alias) => {
                if table_alias == target_table_alias {
                    return columns.iter().cloned().map(Ok).collect();
                }

                let labels = join_columns
                    .iter()
                    .find(|(table_alias, _)| table_alias == &target_table_alias)
                    .map(|(_, columns)| columns.clone());

                match labels {
                    Some(columns) => columns.into_iter().map(Ok).collect(),
                    None => {
                        vec![Err(FetchError::TableAliasNotFound(
                            target_table_alias.to_owned(),
                        )
                        .into())]
                    }
                }
            }
            SelectItem::Expr { label, .. } => vec![Ok(label.to_owned())],
        })
        .collect::<Result<_>>()
        .map(Some)
}
