use boolinator::Boolinator;
use futures::stream::{self, Stream, StreamExt, TryStream, TryStreamExt};
use iter_enum::Iterator;
use serde::Serialize;
use std::fmt::Debug;
use std::iter::once;
use std::rc::Rc;
use thiserror::Error as ThisError;

use sqlparser::ast::{Expr, Ident, Query, SelectItem, SetExpr, TableWithJoins};

use super::aggregate::Aggregate;
use super::blend::Blend;
use super::context::{BlendContext, FilterContext};
use super::fetch::fetch_columns;
use super::filter::Filter;
use super::join::Join;
use super::limit::Limit;
use crate::data::{get_name, Row, Table};
use crate::result::{Error, Result};
use crate::store::Store;

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum SelectError {
    #[error("unimplemented! select on two or more than tables are not supported")]
    TooManyTables,

    #[error("table alias not found: {0}")]
    TableAliasNotFound(String),

    #[error("unreachable!")]
    Unreachable,
}

fn fetch_blended<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table: Table<'a>,
    columns: Rc<Vec<Ident>>,
) -> Result<impl Stream<Item = Result<BlendContext<'a>>> + 'a> {
    let rows = storage.scan_data(table.get_name())?.map(move |data| {
        let (_, row) = data?;
        let row = Some(row);
        let columns = Rc::clone(&columns);

        Ok(BlendContext::new(table.get_alias(), columns, row, None))
    });

    Ok(stream::iter(rows))
}

fn get_labels<'a>(
    projection: &[SelectItem],
    table_alias: &str,
    columns: &'a [Ident],
    join_columns: &'a [(&String, Vec<Ident>)],
) -> Result<Vec<String>> {
    #[derive(Iterator)]
    enum Labeled<I1, I2, I3, I4> {
        Err(I1),
        Wildcard(I2),
        QualifiedWildcard(I3),
        Once(I4),
    };

    let err = |e| Labeled::Err(once(Err(e)));

    macro_rules! try_into {
        ($v: expr) => {
            match $v {
                Ok(v) => v,
                Err(e) => {
                    return err(e);
                }
            }
        };
    }

    let to_labels = |columns: &'a [Ident]| columns.iter().map(|ident| ident.value.to_string());

    projection
        .iter()
        .flat_map(|item| match item {
            SelectItem::Wildcard => {
                let labels = to_labels(columns);
                let join_labels = join_columns
                    .iter()
                    .flat_map(|(_, columns)| to_labels(columns));
                let labels = labels.chain(join_labels).map(Ok);

                Labeled::Wildcard(labels)
            }
            SelectItem::QualifiedWildcard(target) => {
                let target_table_alias = try_into!(get_name(target));

                if table_alias == target_table_alias {
                    return Labeled::QualifiedWildcard(to_labels(columns).map(Ok));
                }

                let columns = join_columns
                    .iter()
                    .find(|(table_alias, _)| table_alias == &target_table_alias)
                    .map(|(_, columns)| columns)
                    .ok_or_else(|| {
                        SelectError::TableAliasNotFound(target_table_alias.to_string()).into()
                    });
                let columns = try_into!(columns);
                let labels = to_labels(columns).map(Ok);

                Labeled::QualifiedWildcard(labels)
            }
            SelectItem::UnnamedExpr(expr) => {
                let label = match expr {
                    Expr::CompoundIdentifier(idents) => try_into!(idents
                        .last()
                        .map(|ident| ident.value.to_string())
                        .ok_or_else(|| SelectError::Unreachable.into())),
                    _ => expr.to_string(),
                };

                Labeled::Once(once(Ok(label)))
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                Labeled::Once(once(Ok(alias.value.to_string())))
            }
        })
        .collect::<Result<_>>()
}

pub async fn select_with_labels<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    query: &'a Query,
    filter_context: Option<Rc<FilterContext<'a>>>,
    with_labels: bool,
) -> Result<(Vec<String>, impl TryStream<Ok = Row, Error = Error> + 'a)> {
    macro_rules! err {
        ($err: expr) => {{
            return Err($err.into());
        }};
    }

    let (table_with_joins, where_clause, projection, group_by, having) = match &query.body {
        SetExpr::Select(statement) => {
            let tables = &statement.from;
            let table_with_joins = match tables.len() {
                1 => &tables[0],
                0 => err!(SelectError::Unreachable),
                _ => err!(SelectError::TooManyTables),
            };

            (
                table_with_joins,
                statement.selection.as_ref(),
                statement.projection.as_ref(),
                &statement.group_by,
                statement.having.as_ref(),
            )
        }
        _ => err!(SelectError::Unreachable),
    };

    let TableWithJoins { relation, joins } = &table_with_joins;
    let table = Table::new(relation)?;

    let columns = fetch_columns(storage, table.get_name())?;
    let join_columns = joins
        .iter()
        .map(|join| {
            let table = Table::new(&join.relation)?;
            let table_alias = table.get_alias();
            let table_name = table.get_name();
            let columns = fetch_columns(storage, table_name)?;

            Ok((table_alias, columns))
        })
        .collect::<Result<Vec<_>>>()?;

    let labels = if with_labels {
        get_labels(&projection, table.get_alias(), &columns, &join_columns)?
    } else {
        vec![]
    };

    let columns = Rc::new(columns);
    let join_columns = join_columns
        .into_iter()
        .map(|(_, columns)| columns)
        .map(Rc::new)
        .collect::<Vec<_>>();
    let join_columns = Rc::new(join_columns);

    let join = Rc::new(Join::new(
        storage,
        joins,
        filter_context.as_ref().map(Rc::clone),
    ));
    let aggregate = Aggregate::new(
        storage,
        projection,
        group_by,
        having,
        filter_context.as_ref().map(Rc::clone),
    );
    let blend = Rc::new(Blend::new(storage, projection));
    let filter = Rc::new(Filter::new(storage, where_clause, filter_context, None));
    let limit = Rc::new(Limit::new(query.limit.as_ref(), query.offset.as_ref())?);

    let rows = fetch_blended(storage, table, columns)?
        .then(move |blend_context| {
            let join_columns = Rc::clone(&join_columns);
            let join = Rc::clone(&join);

            async move { join.apply(blend_context, join_columns).await }
        })
        .try_flatten()
        .try_filter_map(move |blend_context| {
            let filter = Rc::clone(&filter);

            async move {
                filter
                    .check(Rc::clone(&blend_context))
                    .await
                    .map(|pass| pass.as_some(blend_context))
            }
        })
        .enumerate()
        .filter_map(move |(i, item)| {
            let limit = Rc::clone(&limit);

            async move { limit.check(i).as_some(item) }
        });

    let rows = aggregate
        .apply(rows)
        .await?
        .into_stream()
        .and_then(move |aggregate_context| {
            let blend = Rc::clone(&blend);

            async move { blend.apply(Ok(aggregate_context)).await }
        });

    Ok((labels, rows))
}

pub async fn select<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    query: &'a Query,
    filter_context: Option<Rc<FilterContext<'a>>>,
) -> Result<impl TryStream<Ok = Row, Error = Error> + 'a> {
    select_with_labels(storage, query, filter_context, false)
        .await
        .map(|(_, rows)| rows)
}
