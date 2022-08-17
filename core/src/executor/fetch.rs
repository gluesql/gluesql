use {
    super::{context::FilterContext, filter::check_expr},
    crate::{
        ast::{
            ColumnDef, Expr, IndexItem, Join, Query, Select, SetExpr, TableFactor, TableWithJoins,
        },
        data::{get_alias, get_index, get_name, Key, Row, TableError, Value},
        executor::{
            evaluate::evaluate,
            select::{get_labels, select},
        },
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
}

pub async fn fetch<'a>(
    storage: &'a dyn GStore,
    table_name: &'a str,
    columns: Rc<[String]>,
    where_clause: Option<&'a Expr>,
) -> Result<impl TryStream<Ok = (Rc<[String]>, Key, Row), Error = Error> + 'a> {
    let rows = storage
        .scan_data(table_name)
        .await
        .map(stream::iter)?
        .try_filter_map(move |(key, row)| {
            let columns = Rc::clone(&columns);

            async move {
                let expr = match where_clause {
                    None => {
                        return Ok(Some((columns, key, row)));
                    }
                    Some(expr) => expr,
                };

                let context = FilterContext::new(table_name, Rc::clone(&columns), Some(&row), None);

                check_expr(storage, Some(Rc::new(context)), None, expr)
                    .await
                    .map(|pass| pass.then_some((columns, key, row)))
            }
        });

    Ok(rows)
}

#[derive(futures_enum::Stream)]
pub enum Rows<I1, I2, I3> {
    Derived(I1),
    Table(I2),
    Dummy(I3),
}

pub async fn fetch_relation_rows<'a>(
    storage: &'a dyn GStore,
    table_factor: &'a TableFactor,
    filter_context: &Option<Rc<FilterContext<'a>>>,
) -> Result<impl TryStream<Ok = Row, Error = Error, Item = Result<Row>> + 'a> {
    match table_factor {
        TableFactor::Derived { subquery, .. } => {
            let filter_context = filter_context.as_ref().map(Rc::clone);
            let rows = select(storage, subquery, filter_context).await?;

            Ok(Rows::Derived(rows))
        }
        TableFactor::Table { name, .. } => {
            let table_name = get_name(name)?;
            if table_name == "$Dummy" {
                let rows = vec![Ok(Row(vec![]))];
                let rows = stream::iter(rows);

                return Ok(Rows::Dummy(rows));
            }
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
                            .scan_indexed_data(table_name, index_name, *asc, cmp_value)
                            .await?
                            .map_ok(|(_, row)| row);

                        Rows::Indexed(rows)
                    }
                    Some(IndexItem::PrimaryKey(expr)) => {
                        let filter_context = filter_context.as_ref().map(Rc::clone);
                        let key = evaluate(storage, filter_context, None, expr)
                            .await
                            .and_then(Value::try_from)
                            .and_then(Key::try_from)?;

                        let rows = storage
                            .fetch_data(table_name, &key)
                            .await
                            .transpose()
                            .map(|row| vec![row])
                            .unwrap_or_else(Vec::new);

                        Rows::PrimaryKey(rows.into_iter())
                    }
                    _ => {
                        let rows = storage.scan_data(table_name).await?.map_ok(|(_, row)| row);

                        Rows::FullScan(rows)
                    }
                }
            };

            Ok(Rows::Table(stream::iter(rows)))
        }
    }
}

pub async fn fetch_columns(storage: &dyn GStore, table_name: &str) -> Result<Vec<String>> {
    match table_name {
        "$Dummy" => Ok(vec![]),
        _ => Ok(storage
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| FetchError::TableNotFound(table_name.to_string()))?
            .column_defs
            .into_iter()
            .map(|ColumnDef { name, .. }| name)
            .collect::<Vec<String>>()),
    }
}

#[async_recursion(?Send)]
pub async fn fetch_relation_columns(
    storage: &dyn GStore,
    table_factor: &TableFactor,
) -> Result<Vec<String>> {
    match table_factor {
        TableFactor::Table { name, .. } => {
            let table_name = get_name(name)?;

            fetch_columns(storage, table_name).await
        }
        TableFactor::Derived {
            subquery:
                Query {
                    body: SetExpr::Select(statement),
                    ..
                },
            alias: _,
        } => {
            let Select {
                from: TableWithJoins {
                    relation, joins, ..
                },
                projection,
                ..
            } = statement.as_ref();

            let columns = fetch_relation_columns(storage, relation).await?;
            let join_columns = fetch_join_columns(joins, storage).await?;
            let labels = get_labels(
                projection,
                get_alias(relation)?,
                &columns,
                Some(&join_columns),
            )?;

            Ok(labels)
        }
        &TableFactor::Derived { .. } => Err(Error::Table(TableError::Unreachable)),
    }
}
pub async fn fetch_join_columns<'a>(
    joins: &'a [Join],
    storage: &dyn GStore,
) -> Result<Vec<(&'a String, Vec<String>)>> {
    stream::iter(joins.iter())
        .map(Ok::<_, Error>)
        .and_then(|join| async move {
            let relation = &join.relation;
            let alias = get_alias(relation)?;
            let columns = fetch_relation_columns(storage, relation).await?;
            Ok((alias, columns))
        })
        .try_collect::<Vec<_>>()
        .await
}
