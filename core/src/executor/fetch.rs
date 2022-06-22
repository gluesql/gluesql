use {
    super::{context::FilterContext, filter::check_expr},
    crate::{
        ast::{ColumnDef, Expr, Join, Query, Select, SetExpr, TableFactor, TableWithJoins},
        data::{Key, Relation, Row, TableError},
        executor::select::{get_labels, select},
        result::{Error, Result},
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStream, TryStreamExt},
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
pub async fn fetch_relation<'a>(
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
            let rows = storage
                .scan_data(crate::data::get_name(name)?)
                .await?
                .map_ok(|(_, row)| row);
            let rows = stream::iter(rows);

            Ok(Rows::Table(rows))
        }
    }
}

pub async fn fetch_join_columns<'a>(
    joins: &'a Vec<Join>,
    storage: &dyn GStore,
) -> Result<Vec<(&'a String, Vec<String>)>> {
    stream::iter(joins.iter())
        .map(Ok::<_, Error>)
        .and_then(|join| async move {
            match &join.relation {
                TableFactor::Table { .. } => {
                    let relation = Relation::new(&join.relation)?;
                    let table_alias = relation.get_alias();
                    let table_name = relation.get_name();
                    let columns = fetch_columns(storage, table_name).await?;

                    Ok((table_alias, columns))
                }
                TableFactor::Derived {
                    subquery:
                        Query {
                            body: SetExpr::Select(statement),
                            ..
                        },
                    alias,
                } => {
                    let Select {
                        from: TableWithJoins { relation, .. },
                        ..
                    } = statement.as_ref();
                    let Select { projection, .. } = statement.as_ref();
                    let inner_table_name = Relation::new(relation)?.get_name();
                    let columns = fetch_columns(storage, inner_table_name).await?;
                    let columns = get_labels(projection, inner_table_name, &columns, None)?;
                    Ok((&alias.name, columns))
                }
                _ => Err(Error::Table(TableError::Unreachable)),
            }
        })
        .try_collect::<Vec<_>>()
        .await
}
