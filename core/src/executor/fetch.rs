use {
    super::{context::RowContext, evaluate_stateless, filter::check_expr},
    crate::{
        ast::{
            ColumnDef, ColumnOption, Dictionary, Expr, IndexItem, Join, Query, Select, SelectItem,
            SetExpr, TableAlias, TableFactor, TableWithJoins, ToSql, Values,
        },
        data::{get_alias, get_index, Key, Row, Value},
        executor::{evaluate::evaluate, select::select},
        result::{Error, Result},
        store::GStore,
    },
    async_recursion::async_recursion,
    futures::stream::{self, StreamExt, TryStream, TryStreamExt},
    iter_enum::Iterator,
    itertools::Itertools,
    serde::Serialize,
    std::{fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq)]
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

pub async fn fetch<'a>(
    storage: &'a dyn GStore,
    table_name: &'a str,
    columns: Rc<[String]>,
    where_clause: Option<&'a Expr>,
) -> Result<impl TryStream<Ok = (Key, Row), Error = Error> + 'a> {
    let rows = storage
        .scan_data(table_name)
        .await
        .map(stream::iter)?
        .try_filter_map(move |(key, values)| {
            let row = Row {
                columns: Rc::clone(&columns),
                values,
            };

            async move {
                let expr = match where_clause {
                    None => {
                        return Ok(Some((key, row)));
                    }
                    Some(expr) => expr,
                };

                let context = RowContext::new(table_name, &row, None);

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

pub async fn fetch_relation_rows<'a>(
    storage: &'a dyn GStore,
    table_factor: &'a TableFactor,
    filter_context: &Option<Rc<RowContext<'a>>>,
) -> Result<impl TryStream<Ok = Row, Error = Error, Item = Result<Row>> + 'a> {
    let columns = fetch_relation_columns(storage, table_factor)
        .await
        .map(Rc::from)?;

    match table_factor {
        TableFactor::Derived { subquery, .. } => {
            let filter_context = filter_context.as_ref().map(Rc::clone);
            let rows = select(storage, subquery, filter_context).await?;

            Ok(Rows::Derived(rows))
        }
        TableFactor::Table { name, .. } => {
            let rows = {
                #[cfg(feature = "index")]
                #[derive(Iterator)]
                enum Rows<I1, I2, I3> {
                    Indexed(I1),
                    PrimaryKey(I2),
                    FullScan(I3),
                }
                #[cfg(not(feature = "index"))]
                #[derive(Iterator)]
                enum Rows<I1, I2> {
                    PrimaryKey(I1),
                    FullScan(I2),
                }

                match get_index(table_factor) {
                    #[cfg(feature = "index")]
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
                            .map_ok(move |(_, values)| Row {
                                columns: Rc::clone(&columns),
                                values,
                            });

                        Rows::Indexed(rows)
                    }
                    Some(IndexItem::PrimaryKey(expr)) => {
                        let filter_context = filter_context.as_ref().map(Rc::clone);
                        let key = evaluate(storage, filter_context, None, expr)
                            .await
                            .and_then(Value::try_from)
                            .and_then(Key::try_from)?;

                        let rows = storage
                            .fetch_data(name, &key)
                            .await
                            .transpose()
                            .map(|row| vec![row])
                            .unwrap_or_else(Vec::new);

                        Rows::PrimaryKey(rows.into_iter().map_ok(move |values| Row {
                            columns: Rc::clone(&columns),
                            values,
                        }))
                    }
                    _ => {
                        let rows = storage
                            .scan_data(name)
                            .await?
                            .map_ok(move |(_, values)| Row {
                                columns: Rc::clone(&columns),
                                values,
                            });

                        Rows::FullScan(rows)
                    }
                }
            };

            Ok(Rows::Table(stream::iter(rows)))
        }
        TableFactor::Series { size, .. } => {
            let value: Value = evaluate_stateless(None, size)?.try_into()?;
            let size: i64 = value.try_into()?;
            let size = match size {
                n if n >= 0 => size,
                n => return Err(FetchError::SeriesSizeWrong(n).into()),
            };

            let columns = Rc::from(vec!["N".to_owned()]);
            let rows = (1..=size).map(move |v| {
                Ok(Row {
                    columns: Rc::clone(&columns),
                    values: vec![Value::I64(v)],
                })
            });

            Ok(Rows::Series(stream::iter(rows)))
        }
        TableFactor::Dictionary { dict, .. } => {
            let rows = {
                #[derive(Iterator)]
                enum Rows<I1, I2, I3, I4> {
                    Tables(I1),
                    TableColumns(I2),
                    Indexes(I3),
                    Objects(I4),
                }
                match dict {
                    Dictionary::GlueObjects => {
                        let schemas = storage.fetch_all_schemas().await?;
                        let rows = schemas.into_iter().flat_map(move |schema| {
                            let table_rows = vec![Ok(Row {
                                columns: Rc::clone(&columns),
                                values: vec![
                                    Value::Str(schema.table_name),
                                    Value::Str("TABLE".to_owned()),
                                    Value::Timestamp(schema.created),
                                ],
                            })];

                            let columns = Rc::clone(&columns);
                            let index_rows = schema.indexes.into_iter().map(move |index| {
                                let values = vec![
                                    Value::Str(index.name.clone()),
                                    Value::Str("INDEX".to_owned()),
                                    Value::Timestamp(index.created),
                                ];

                                Ok(Row {
                                    columns: Rc::clone(&columns),
                                    values,
                                })
                            });

                            table_rows.into_iter().chain(index_rows)
                        });

                        Rows::Objects(rows)
                    }
                    Dictionary::GlueTables => {
                        let schemas = storage.fetch_all_schemas().await?;
                        let rows = schemas.into_iter().map(move |schema| {
                            Ok(Row {
                                columns: Rc::clone(&columns),
                                values: vec![Value::Str(schema.table_name)],
                            })
                        });

                        Rows::Tables(rows)
                    }
                    Dictionary::GlueTableColumns => {
                        let schemas = storage.fetch_all_schemas().await?;
                        let rows = schemas.into_iter().flat_map(move |schema| {
                            let columns = Rc::clone(&columns);
                            let table_name = schema.table_name;

                            schema.column_defs.into_iter().enumerate().map(
                                move |(index, column_def)| {
                                    let values = vec![
                                        Value::Str(table_name.clone()),
                                        Value::Str(column_def.name),
                                        Value::I64(index as i64 + 1),
                                    ];

                                    Ok(Row {
                                        columns: Rc::clone(&columns),
                                        values,
                                    })
                                },
                            )
                        });

                        Rows::TableColumns(rows)
                    }
                    Dictionary::GlueIndexes => {
                        let schemas = storage.fetch_all_schemas().await?;
                        let rows = schemas.into_iter().flat_map(move |schema| {
                            let primary_column = schema.column_defs.iter().find_map(
                                |ColumnDef { name, options, .. }| {
                                    options
                                        .iter()
                                        .any(|option| {
                                            option == &ColumnOption::Unique { is_primary: true }
                                        })
                                        .then_some(name)
                                },
                            );

                            let clustered = match primary_column {
                                Some(column_name) => {
                                    let values = vec![
                                        Value::Str(schema.table_name.clone()),
                                        Value::Str("PRIMARY".to_owned()),
                                        Value::Str("BOTH".to_owned()),
                                        Value::Str(column_name.to_owned()),
                                        Value::Bool(true),
                                    ];

                                    let row = Row {
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
                                    Value::Str(index.expr.to_sql()),
                                    Value::Bool(false),
                                ];

                                Ok(Row {
                                    columns: Rc::clone(&columns),
                                    values,
                                })
                            });

                            clustered.into_iter().chain(non_clustered)
                        });

                        Rows::Indexes(rows)
                    }
                }
            };

            Ok(Rows::Dictionary(stream::iter(rows)))
        }
    }
}

pub async fn fetch_columns(storage: &dyn GStore, table_name: &str) -> Result<Vec<String>> {
    Ok(storage
        .fetch_schema(table_name)
        .await?
        .ok_or_else(|| FetchError::TableNotFound(table_name.to_owned()))?
        .column_defs
        .into_iter()
        .map(|ColumnDef { name, .. }| name)
        .collect::<Vec<String>>())
}

#[async_recursion(?Send)]
pub async fn fetch_relation_columns(
    storage: &dyn GStore,
    table_factor: &TableFactor,
) -> Result<Vec<String>> {
    match table_factor {
        TableFactor::Table { name, .. } => fetch_columns(storage, name).await,
        TableFactor::Series { .. } => Ok(vec!["N".to_owned()]),
        TableFactor::Dictionary { dict, .. } => match dict {
            Dictionary::GlueObjects => Ok(vec![
                "OBJECT_NAME".to_owned(),
                "OBJECT_TYPE".to_owned(),
                "CREATED".to_owned(),
            ]),
            Dictionary::GlueTables => Ok(vec!["TABLE_NAME".to_owned()]),
            Dictionary::GlueTableColumns => Ok(vec![
                "TABLE_NAME".to_owned(),
                "COLUMN_NAME".to_owned(),
                "COLUMN_ID".to_owned(),
            ]),
            Dictionary::GlueIndexes => Ok(vec![
                "TABLE_NAME".to_owned(),
                "INDEX_NAME".to_owned(),
                "ORDER".to_owned(),
                "EXPRESSION".to_owned(),
                "UNIQUENESS".to_owned(),
            ]),
        },
        TableFactor::Derived {
            subquery: Query { body, .. },
            alias: TableAlias { columns, name },
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

                fetch_labels(storage, relation, joins, projection).await
            }
            SetExpr::Values(Values(values_list)) => {
                let total_len = values_list[0].len();
                let alias_len = columns.len();
                if alias_len > total_len {
                    return Err(FetchError::TooManyColumnAliases(
                        name.into(),
                        total_len,
                        alias_len,
                    )
                    .into());
                }
                let labels = (alias_len + 1..=total_len)
                    .into_iter()
                    .map(|i| format!("column{}", i));
                let labels = columns.iter().cloned().chain(labels).collect::<Vec<_>>();

                Ok(labels)
            }
        },
    }
}

async fn fetch_join_columns<'a>(
    joins: &'a [Join],
    storage: &dyn GStore,
) -> Result<Vec<(&'a String, Vec<String>)>> {
    stream::iter(joins.iter())
        .map(Ok::<_, Error>)
        .and_then(|join| async move {
            let relation = &join.relation;
            let alias = get_alias(relation);
            let columns = fetch_relation_columns(storage, relation).await?;
            Ok((alias, columns))
        })
        .try_collect::<Vec<_>>()
        .await
}

pub async fn fetch_labels<'a>(
    storage: &dyn GStore,
    relation: &'a TableFactor,
    joins: &'a [Join],
    projection: &'a [SelectItem],
) -> Result<Vec<String>> {
    let table_alias = get_alias(relation);
    let columns = fetch_relation_columns(storage, relation)
        .await
        .map(Rc::new)?;
    let join_columns = fetch_join_columns(joins, storage).await.map(Rc::new)?;

    projection
        .iter()
        .flat_map(move |item| {
            let columns = Rc::clone(&columns);
            let join_columns = Rc::clone(&join_columns);

            match item {
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
                            let e = FetchError::TableAliasNotFound(target_table_alias.to_owned());

                            return vec![Err(e.into())];
                        }
                    }
                }
                SelectItem::Expr { label, .. } => vec![Ok(label.to_owned())],
            }
        })
        .collect::<Result<_>>()
}
