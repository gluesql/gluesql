use boolinator::Boolinator;
use iter_enum::Iterator;
use serde::Serialize;
use std::fmt::Debug;
use std::iter::once;
use std::rc::Rc;
use thiserror::Error;

use sqlparser::ast::{Expr, Ident, Query, SelectItem, SetExpr, TableWithJoins};

use super::aggregate::Aggregate;
use super::blend::Blend;
use super::context::{BlendContext, FilterContext};
use super::fetch::fetch_columns;
use super::filter::Filter;
use super::join::Join;
use super::limit::Limit;
use crate::data::{get_name, Row, Table};
use crate::result::Result;
use crate::store::Store;

#[derive(Error, Serialize, Debug, PartialEq)]
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
) -> Result<impl Iterator<Item = Result<BlendContext<'a>>> + 'a> {
    let rows = storage.scan_data(table.get_name())?.map(move |data| {
        let (_, row) = data?;
        let row = Some(row);
        let columns = Rc::clone(&columns);

        Ok(BlendContext::new(table.get_alias(), columns, row, None))
    });

    Ok(rows)
}

fn get_aliases<'a>(
    projection: &[SelectItem],
    table_alias: &str,
    columns: &'a [Ident],
    join_columns: &'a [(&String, Vec<Ident>)],
) -> Result<Vec<String>> {
    #[derive(Iterator)]
    enum Aliased<I1, I2, I3, I4> {
        Err(I1),
        Wildcard(I2),
        QualifiedWildcard(I3),
        Once(I4),
    };

    let err = |e| Aliased::Err(once(Err(e)));

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

    let to_aliases = |columns: &'a [Ident]| columns.iter().map(|ident| ident.value.to_string());

    projection
        .iter()
        .flat_map(|item| match item {
            SelectItem::Wildcard => {
                let columns = to_aliases(columns);
                let join_columns = join_columns
                    .iter()
                    .flat_map(|(_, columns)| to_aliases(columns));
                let columns = columns.chain(join_columns).map(Ok);

                Aliased::Wildcard(columns)
            }
            SelectItem::QualifiedWildcard(target) => {
                let target_table_alias = try_into!(get_name(target));

                if table_alias == target_table_alias {
                    return Aliased::QualifiedWildcard(to_aliases(columns).map(Ok));
                }

                let columns = join_columns
                    .iter()
                    .find(|(table_alias, _)| table_alias == &target_table_alias)
                    .map(|(_, columns)| columns)
                    .ok_or_else(|| {
                        SelectError::TableAliasNotFound(target_table_alias.to_string()).into()
                    });
                let columns = try_into!(columns);

                Aliased::QualifiedWildcard(to_aliases(columns).map(Ok))
            }
            SelectItem::UnnamedExpr(expr) => {
                let alias = match expr {
                    Expr::CompoundIdentifier(idents) => try_into!(idents
                        .last()
                        .map(|ident| ident.value.to_string())
                        .ok_or_else(|| SelectError::Unreachable.into())),
                    _ => expr.to_string(),
                };

                Aliased::Once(once(Ok(alias)))
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                Aliased::Once(once(Ok(alias.value.to_string())))
            }
        })
        .collect::<Result<_>>()
}

pub fn select_with_aliases<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    query: &'a Query,
    filter_context: Option<Rc<FilterContext<'a>>>,
    with_aliases: bool,
) -> Result<(Vec<String>, impl Iterator<Item = Result<Row>> + 'a)> {
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

    let aliases = if with_aliases {
        get_aliases(&projection, table.get_alias(), &columns, &join_columns)?
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

    let join = Join::new(storage, joins, filter_context.as_ref().map(Rc::clone));
    let aggregate = Aggregate::new(
        storage,
        projection,
        group_by,
        having,
        filter_context.as_ref().map(Rc::clone),
    );
    let blend = Blend::new(storage, projection);
    let filter = Filter::new(storage, where_clause, filter_context, None);
    let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref())?;

    let rows = fetch_blended(storage, table, columns)?
        .flat_map(move |blend_context| {
            let join_columns = Rc::clone(&join_columns);

            join.apply(blend_context, join_columns)
        })
        .filter_map(move |blend_context| {
            blend_context.map_or_else(
                |error| Some(Err(error)),
                |blend_context| {
                    filter
                        .check_blended(&blend_context)
                        .map(|pass| pass.as_some(blend_context))
                        .transpose()
                },
            )
        })
        .enumerate()
        .filter_map(move |(i, item)| limit.check(i).as_some(item));

    let rows = {
        let rows = aggregate.apply(rows)?;
        let rows = Box::new(rows);

        rows.map(move |blend_context| blend.apply(blend_context))
    };

    Ok((aliases, rows))
}

pub fn select<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    query: &'a Query,
    filter_context: Option<Rc<FilterContext<'a>>>,
) -> Result<impl Iterator<Item = Result<Row>> + 'a> {
    let (_, rows) = select_with_aliases(storage, query, filter_context, false)?;

    Ok(rows)
}
