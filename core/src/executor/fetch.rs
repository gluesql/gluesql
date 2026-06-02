use {
    super::{context::RowContext, evaluate::evaluate_stateless, filter::check_expr},
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, Dictionary, ToSql, ToSqlUnquoted},
        data::{Key, Row, SCHEMALESS_DOC_COLUMN, Value},
        executor::{evaluate::evaluate, select::select},
        plan::{
            ExprPlan, IndexItemPlan, JoinPlan, ProjectionPlan, QueryPlan, SelectItemPlan,
            SetExprPlan, TableAliasPlan, TableFactorPlan, TableWithJoinsPlan, ValuesPlan,
        },
        result::Result,
        store::GStore,
    },
    serde::Serialize,
    std::{borrow::Cow, collections::BTreeMap, fmt::Debug, iter, sync::Arc},
    thiserror::Error as ThisError,
};

pub type KeyedRows<'a> = Box<dyn Iterator<Item = Result<(Key, Row)>> + Send + 'a>;
pub type RelationRows<'a> = Box<dyn Iterator<Item = Result<Row>> + Send + 'a>;

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

    #[error("unreachable")]
    Unreachable,
}

pub fn fetch<'a, T: GStore>(
    storage: &'a T,
    table_name: &'a str,
    columns: Arc<[String]>,
    where_clause: Option<&'a ExprPlan>,
) -> Result<KeyedRows<'a>> {
    let rows = storage.scan_data(table_name)?.filter_map(move |row| {
        let (key, values) = match row {
            Ok(row) => row,
            Err(error) => return Some(Err(error)),
        };
        let row = Row {
            columns: Arc::clone(&columns),
            values,
        };

        match where_clause {
            Some(expr) => {
                let context = RowContext::new(table_name, Cow::Borrowed(&row), None);
                let context = Arc::new(context);
                match check_expr(storage, Some(&context), None, expr) {
                    Ok(true) => Some(Ok((key, row))),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                }
            }
            None => Some(Ok((key, row))),
        }
    });

    Ok(Box::new(rows))
}

pub fn fetch_relation_rows<'a, T: GStore>(
    storage: &'a T,
    table_factor: &'a TableFactorPlan,
    filter_context: Option<&Arc<RowContext<'a>>>,
) -> Result<RelationRows<'a>> {
    let columns = Arc::from(fetch_relation_columns(storage, table_factor)?);

    match table_factor {
        TableFactorPlan::Derived { subquery, .. } => {
            let filter_context = filter_context.map(Arc::clone);
            let rows = select(storage, subquery, filter_context)?.map(move |row| {
                let row = row?;
                Ok(Row {
                    columns: Arc::clone(&columns),
                    values: row.values,
                })
            });

            Ok(Box::new(rows))
        }
        TableFactorPlan::Table { name, .. } => {
            let rows = match table_factor.index() {
                Some(IndexItemPlan::NonClustered {
                    name: index_name,
                    asc,
                    cmp_expr,
                }) => {
                    let cmp_value = match cmp_expr {
                        Some((op, expr)) => {
                            let evaluated = evaluate(storage, None, None, expr)?;

                            Some((op, evaluated.try_into()?))
                        }
                        None => None,
                    };

                    let rows = storage
                        .scan_indexed_data(name, index_name, *asc, cmp_value)?
                        .map(move |row| {
                            let (_, values) = row?;
                            Ok(Row {
                                columns: Arc::clone(&columns),
                                values,
                            })
                        });
                    Box::new(rows) as Box<dyn Iterator<Item = Result<Row>> + Send + 'a>
                }
                Some(IndexItemPlan::PrimaryKey(expr)) => {
                    let schema = storage.fetch_schema(name)?.ok_or(FetchError::Unreachable)?;

                    let evaluated = evaluate(storage, filter_context, None, expr)?;

                    let column_def = schema
                        .column_defs
                        .as_ref()
                        .and_then(|column_defs| {
                            column_defs
                                .iter()
                                .find(|column_def| column_def.unique.is_some_and(|u| u.is_primary))
                        })
                        .ok_or(FetchError::Unreachable)?;

                    let value =
                        evaluated.try_into_value(&column_def.data_type, column_def.nullable)?;
                    let key = Key::try_from(value)?;

                    match storage.fetch_data(name, &key)? {
                        Some(values) => Box::new(iter::once(Ok(Row {
                            columns: Arc::clone(&columns),
                            values,
                        })))
                            as Box<dyn Iterator<Item = Result<Row>> + Send + 'a>,
                        None => Box::new(iter::empty())
                            as Box<dyn Iterator<Item = Result<Row>> + Send + 'a>,
                    }
                }
                _ => {
                    let rows = storage.scan_data(name)?.map(move |row| {
                        let (_, values) = row?;
                        Ok(Row {
                            columns: Arc::clone(&columns),
                            values,
                        })
                    });
                    Box::new(rows) as Box<dyn Iterator<Item = Result<Row>> + Send + 'a>
                }
            };

            Ok(rows)
        }
        TableFactorPlan::Series { size, .. } => {
            let value: Value = evaluate_stateless(None, size)?.try_into()?;
            let size: i64 = value.try_into()?;
            let size = match size {
                n if n >= 0 => size,
                n => return Err(FetchError::SeriesSizeWrong(n).into()),
            };

            let columns = Arc::from(vec!["N".to_owned()]);
            let rows = (1..=size).map(move |v| {
                Ok(Row {
                    columns: Arc::clone(&columns),
                    values: vec![Value::I64(v)],
                })
            });

            Ok(Box::new(rows))
        }
        TableFactorPlan::Dictionary { dict, .. } => {
            let rows = match dict {
                Dictionary::GlueObjects => {
                    let schemas = storage.fetch_all_schemas()?;
                    let table_metas = storage
                        .scan_table_meta()?
                        .collect::<Result<BTreeMap<_, _>>>()?;
                    let rows = schemas.into_iter().flat_map(move |schema| {
                        let meta = table_metas
                            .iter()
                            .find_map(|(table_name, hash_map)| {
                                (table_name == &schema.table_name).then(|| hash_map.clone())
                            })
                            .unwrap_or_default();

                        let table_rows = BTreeMap::from([
                            ("OBJECT_NAME".to_owned(), Value::Str(schema.table_name)),
                            ("OBJECT_TYPE".to_owned(), Value::Str("TABLE".to_owned())),
                        ])
                        .into_iter()
                        .chain(meta)
                        .collect::<BTreeMap<_, _>>();

                        let index_rows = schema.indexes.into_iter().map(|index| {
                            BTreeMap::from([
                                ("OBJECT_NAME".to_owned(), Value::Str(index.name)),
                                ("OBJECT_TYPE".to_owned(), Value::Str("INDEX".to_owned())),
                            ])
                        });

                        iter::once(table_rows).chain(index_rows).map(|hash_map| {
                            let (columns, values): (Vec<_>, Vec<_>) = hash_map.into_iter().unzip();
                            Row {
                                columns: columns.into(),
                                values,
                            }
                        })
                    });

                    Box::new(rows.map(Ok)) as Box<dyn Iterator<Item = Result<Row>> + Send + 'a>
                }
                Dictionary::GlueTables => {
                    let schemas = storage.fetch_all_schemas()?;
                    let rows = schemas.into_iter().map(move |schema| Row {
                        columns: Arc::clone(&columns),
                        values: vec![
                            Value::Str(schema.table_name),
                            schema.comment.map_or(Value::Null, Value::Str),
                        ],
                    });

                    Box::new(rows.map(Ok))
                }
                Dictionary::GlueTableColumns => {
                    let schemas = storage.fetch_all_schemas()?;
                    let rows = schemas.into_iter().flat_map(move |schema| {
                        let columns = Arc::clone(&columns);
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
                                    column_def
                                        .unique
                                        .map_or(Value::Null, |unique| Value::Str(unique.to_sql())),
                                    column_def
                                        .default
                                        .map_or(Value::Null, |expr| Value::Str(expr.to_sql())),
                                    column_def.comment.map_or(Value::Null, Value::Str),
                                ];

                                Row {
                                    columns: Arc::clone(&columns),
                                    values,
                                }
                            })
                    });

                    Box::new(rows.map(Ok))
                }
                Dictionary::GlueIndexes => {
                    let schemas = storage.fetch_all_schemas()?;
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

                                let row = Row {
                                    columns: Arc::clone(&columns),
                                    values,
                                };

                                vec![row]
                            }
                            None => Vec::new(),
                        };

                        let columns = Arc::clone(&columns);
                        let non_clustered = schema.indexes.into_iter().map(move |index| {
                            let values = vec![
                                Value::Str(schema.table_name.clone()),
                                Value::Str(index.name),
                                Value::Str(index.order.to_string()),
                                Value::Str(index.expr.to_sql_unquoted()),
                                Value::Bool(false),
                            ];

                            Row {
                                columns: Arc::clone(&columns),
                                values,
                            }
                        });

                        clustered.into_iter().chain(non_clustered)
                    });

                    Box::new(rows.map(Ok))
                }
            };

            Ok(rows)
        }
    }
}

pub fn fetch_columns<T: GStore>(storage: &T, table_name: &str) -> Result<Vec<String>> {
    let columns = storage
        .fetch_schema(table_name)?
        .ok_or_else(|| FetchError::TableNotFound(table_name.to_owned()))?
        .column_defs
        .map_or_else(
            || vec![SCHEMALESS_DOC_COLUMN.to_owned()],
            |column_defs| {
                column_defs
                    .into_iter()
                    .map(|column_def| column_def.name)
                    .collect()
            },
        );

    Ok(columns)
}

pub fn fetch_relation_columns<T>(storage: &T, table_factor: &TableFactorPlan) -> Result<Vec<String>>
where
    T: GStore,
{
    match table_factor {
        TableFactorPlan::Table { name, alias, .. } => {
            let columns = fetch_columns(storage, name)?;
            match alias {
                None => Ok(columns),
                Some(alias) if alias.columns.len() > columns.len() => {
                    Err(FetchError::TooManyColumnAliases(
                        name.clone(),
                        columns.len(),
                        alias.columns.len(),
                    )
                    .into())
                }
                Some(alias) => Ok(alias
                    .columns
                    .iter()
                    .cloned()
                    .chain(columns[alias.columns.len()..columns.len()].to_vec())
                    .collect()),
            }
        }
        TableFactorPlan::Series { .. } => Ok(vec!["N".to_owned()]),
        TableFactorPlan::Dictionary { dict, .. } => Ok(match dict {
            Dictionary::GlueObjects => vec![
                "OBJECT_NAME".to_owned(),
                "OBJECT_TYPE".to_owned(),
                "CREATED".to_owned(),
            ],
            Dictionary::GlueTables => vec!["TABLE_NAME".to_owned(), "COMMENT".to_owned()],
            Dictionary::GlueTableColumns => vec![
                "TABLE_NAME".to_owned(),
                "COLUMN_NAME".to_owned(),
                "COLUMN_ID".to_owned(),
                "NULLABLE".to_owned(),
                "KEY".to_owned(),
                "DEFAULT".to_owned(),
                "COMMENT".to_owned(),
            ],
            Dictionary::GlueIndexes => vec![
                "TABLE_NAME".to_owned(),
                "INDEX_NAME".to_owned(),
                "ORDER".to_owned(),
                "EXPRESSION".to_owned(),
                "UNIQUENESS".to_owned(),
            ],
        }),
        TableFactorPlan::Derived {
            subquery: QueryPlan { body, .. },
            alias:
                TableAliasPlan {
                    columns: alias_columns,
                    name,
                },
        } => match body {
            SetExprPlan::Select(statement) => {
                let crate::plan::SelectPlan {
                    from:
                        TableWithJoinsPlan {
                            relation, joins, ..
                        },
                    projection,
                    ..
                } = statement.as_ref();

                let labels = fetch_labels(storage, relation, joins, projection)?;
                if alias_columns.is_empty() {
                    Ok(labels)
                } else if alias_columns.len() > labels.len() {
                    Err(FetchError::TooManyColumnAliases(
                        name.clone(),
                        labels.len(),
                        alias_columns.len(),
                    )
                    .into())
                } else {
                    Ok(alias_columns
                        .iter()
                        .cloned()
                        .chain(labels[alias_columns.len()..labels.len()].to_vec())
                        .collect())
                }
            }
            SetExprPlan::Values(ValuesPlan(values_list)) => {
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
                let labels = (alias_len + 1..=total_len).map(|i| format!("column{i}"));
                let labels = alias_columns
                    .iter()
                    .cloned()
                    .chain(labels)
                    .collect::<Vec<_>>();

                Ok(labels)
            }
        },
    }
}

fn fetch_join_columns<'a, T: GStore>(
    storage: &T,
    joins: &'a [JoinPlan],
) -> Result<Vec<(&'a str, Vec<String>)>> {
    let mut all_columns = Vec::with_capacity(joins.len());
    for join in joins {
        let columns = fetch_relation_columns(storage, &join.relation)?;
        let alias = join.relation.alias_name();
        all_columns.push((alias, columns));
    }
    Ok(all_columns)
}

pub fn fetch_labels<T: GStore>(
    storage: &T,
    relation: &TableFactorPlan,
    joins: &[JoinPlan],
    projection: &ProjectionPlan,
) -> Result<Vec<String>> {
    let table_alias = relation.alias_name();
    let columns = fetch_relation_columns(storage, relation)?;
    let join_columns = fetch_join_columns(storage, joins)?;

    match projection {
        ProjectionPlan::SchemalessMap => Ok(vec![SCHEMALESS_DOC_COLUMN.to_owned()]),
        ProjectionPlan::SelectItems(projection) => projection
            .iter()
            .flat_map(|item| match item {
                SelectItemPlan::Wildcard => {
                    let columns = columns.iter().cloned();
                    let join_columns = join_columns.iter().flat_map(|(_, columns)| columns.clone());

                    columns.chain(join_columns).map(Ok).collect()
                }
                SelectItemPlan::QualifiedWildcard(target_table_alias) => {
                    if table_alias == target_table_alias {
                        return columns.iter().cloned().map(Ok).collect();
                    }

                    let labels = join_columns
                        .iter()
                        .find(|(table_alias, _)| table_alias == target_table_alias)
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
                SelectItemPlan::Expr { label, .. } => vec![Ok(label.to_owned())],
            })
            .collect::<Result<_>>(),
    }
}
