use {
    super::{
        aggregate::Aggregate,
        blend::Blend,
        context::{BlendContext, FilterContext},
        fetch::fetch_columns,
        filter::Filter,
        join::Join,
        limit::Limit,
    },
    crate::{
        ast::{Query, SelectItem, SetExpr, TableWithJoins},
        data::{get_name, Row, Table},
        result::{Error, Result},
        store::Store,
    },
    boolinator::Boolinator,
    futures::stream::{self, Stream, StreamExt, TryStream, TryStreamExt},
    iter_enum::Iterator,
    serde::Serialize,
    std::{fmt::Debug, iter::once, rc::Rc},
    thiserror::Error as ThisError,
};

#[cfg(feature = "index")]
use {
    super::evaluate::evaluate,
    crate::{ast::IndexItem, data::Value},
    std::convert::TryInto,
};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum SelectError {
    #[error("table alias not found: {0}")]
    TableAliasNotFound(String),

    #[error("unreachable!")]
    Unreachable,
}

async fn fetch_blended<'a, T: 'static + Debug>(
    storage: &dyn Store<T>,
    table: Table<'a>,
    columns: Rc<[String]>,
) -> Result<impl Stream<Item = Result<BlendContext<'a>>> + 'a> {
    let table_name = table.get_name();

    #[cfg(feature = "index")]
    let rows = {
        #[derive(Iterator)]
        enum Rows<I1, I2> {
            FullScan(I1),
            Indexed(I2),
        }

        match table.get_index() {
            Some(IndexItem {
                name: index_name,
                op: index_op,
                value_expr,
            }) => {
                let evaluated = evaluate(storage, None, None, value_expr, false).await?;
                let index_value: Value = evaluated.try_into()?;

                storage
                    .scan_indexed_data(table_name, index_name, index_op, index_value)
                    .await
                    .map(Rows::Indexed)?
            }
            None => storage.scan_data(table_name).await.map(Rows::FullScan)?,
        }
    };
    #[cfg(not(feature = "index"))]
    let rows = storage.scan_data(table_name).await?;

    let rows = rows.map(move |data| {
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
    columns: &'a [String],
    join_columns: &'a [(&String, Vec<String>)],
) -> Result<Vec<String>> {
    #[derive(Iterator)]
    enum Labeled<I1, I2, I3, I4> {
        Err(I1),
        Wildcard(I2),
        QualifiedWildcard(I3),
        Once(I4),
    }

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

    let to_labels = |columns: &'a [String]| columns.iter().map(|ident| ident.to_owned());

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
            SelectItem::Expr { label, .. } => Labeled::Once(once(Ok(label.to_owned()))),
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
        SetExpr::Select(statement) => (
            &statement.from,
            statement.selection.as_ref(),
            statement.projection.as_ref(),
            &statement.group_by,
            statement.having.as_ref(),
        ),
        _ => err!(SelectError::Unreachable),
    };

    let TableWithJoins { relation, joins } = &table_with_joins;
    let table = Table::new(relation)?;

    let columns = fetch_columns(storage, table.get_name()).await?;
    let join_columns = stream::iter(joins.iter())
        .map(Ok::<_, Error>)
        .and_then(|join| {
            let table = Table::new(&join.relation);

            async move {
                let table = table?;
                let table_alias = table.get_alias();
                let table_name = table.get_name();

                let columns = fetch_columns(storage, table_name).await?;

                Ok((table_alias, columns))
            }
        })
        .try_collect::<Vec<_>>()
        .await?;

    let labels = if with_labels {
        get_labels(&projection, table.get_alias(), &columns, &join_columns)?
    } else {
        vec![]
    };

    let columns = Rc::from(columns);
    let join_columns = join_columns
        .into_iter()
        .map(|(_, columns)| columns)
        .map(Rc::from)
        .collect::<Vec<_>>();
    let join_columns = Rc::from(join_columns);

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

    let rows = fetch_blended(storage, table, columns)
        .await?
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
        });

    let rows = limit.apply(rows);

    let rows = aggregate
        .apply(rows)
        .await?
        .into_stream()
        .and_then(move |aggregate_context| {
            let blend = Rc::clone(&blend);

            async move { blend.apply(aggregate_context).await }
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
