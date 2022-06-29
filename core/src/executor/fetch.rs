use {
    super::{context::FilterContext, filter::check_expr},
    crate::{
        ast::{
            ColumnDef, Expr, Join, ObjectName, Query, Select, SetExpr, TableAlias, TableFactor,
            TableWithJoins,
        },
        data::{Key, Row},
        executor::select::{get_labels, select},
        result::{Error, Result},
        store::GStore,
    },
    async_recursion::async_recursion,
    futures::stream::{self, StreamExt, TryStream, TryStreamExt},
    itertools::Itertools,
    serde::Serialize,
    std::{fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[cfg(feature = "index")]
use {super::evaluate::evaluate, iter_enum::Iterator};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum FetchError {
    #[error("table not found: {0}")]
    TableNotFound(String),
}

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum TableError {
    #[error("unreachable")]
    Unreachable,
}

pub async fn fetch_columns(storage: &dyn GStore, table_name: &str) -> Result<Vec<String>> {
    Ok(storage
        .fetch_schema(table_name)
        .await?
        .ok_or_else(|| FetchError::TableNotFound(table_name.to_string()))?
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
        TableFactor::Table { name, .. } => {
            let table_name = fetch_name(name)?;

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
                    .map(|pass| pass.then(|| (columns, key, row)))
            }
        });

    Ok(rows)
}

#[derive(futures_enum::Stream)]
pub enum Rows<I1, I2> {
    Derived(I1),
    Table(I2),
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
            let table_name = fetch_name(name)?;
            #[cfg(feature = "index")]
            let rows = {
                #[derive(Iterator)]
                enum Rows<I1, I2> {
                    FullScan(I1),
                    Indexed(I2),
                }

                match get_index(table_factor) {
                    Some(IndexItem {
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

                        storage
                            .scan_indexed_data(table_name, index_name, *asc, cmp_value)
                            .await
                            .map(Rows::Indexed)?
                    }
                    None => storage.scan_data(table_name).await.map(Rows::FullScan)?,
                }
            };

            #[cfg(not(feature = "index"))]
            let rows = storage.scan_data(table_name).await?;

            let rows = rows.map_ok(|(_, row)| row);
            let rows = stream::iter(rows);

            Ok(Rows::Table(rows))
        }
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

            // match &join.relation {
            //     TableFactor::Table { .. } => {
            //         let table_alias = get_alias(&join.relation)?;
            //         let table_name = get_name(&join.relation)?;
            //         let columns = fetch_columns(storage, table_name).await?;

            //         Ok((table_alias, columns))
            //     }
            //     TableFactor::Derived {
            //         subquery:
            //             Query {
            //                 body: SetExpr::Select(statement),
            //                 ..
            //             },
            //         alias,
            //     } => {
            //         let Select {
            //             from: TableWithJoins { relation, .. },
            //             ..
            //         } = statement.as_ref();
            //         let Select { projection, .. } = statement.as_ref();
            //         let inner_table_name = get_name(relation)?;
            //         let columns = fetch_columns(storage, inner_table_name).await?;
            //         let columns = get_labels(projection, inner_table_name, &columns, None)?;
            //         Ok((&alias.name, columns))
            //     }
            //     _ => Err(Error::Table(TableError::Unreachable)),
            // }
        })
        .try_collect::<Vec<_>>()
        .await
}

pub fn fetch_name(table_name: &ObjectName) -> Result<&String> {
    let ObjectName(idents) = table_name;
    idents.last().ok_or_else(|| TableError::Unreachable.into())
}

pub fn get_name(table_factor: &TableFactor) -> Result<&String> {
    match table_factor {
        TableFactor::Table { name, .. } => fetch_name(name),
        TableFactor::Derived { alias, .. } => Ok(&alias.name),
    }
}

pub fn get_alias(table_factor: &TableFactor) -> Result<&String> {
    match table_factor {
        TableFactor::Table {
            name, alias: None, ..
        } => fetch_name(name),
        TableFactor::Table {
            alias: Some(TableAlias { name, .. }),
            ..
        }
        | TableFactor::Derived {
            alias: TableAlias { name, .. },
            ..
        } => Ok(name),
    }
}

#[cfg(feature = "index")]
use crate::ast::IndexItem;
#[cfg(feature = "index")]
pub fn get_index(table_factor: &TableFactor) -> Option<&IndexItem> {
    match table_factor {
        TableFactor::Table { index, .. } => index.as_ref(),
        TableFactor::Derived { .. } => None,
    }
}
