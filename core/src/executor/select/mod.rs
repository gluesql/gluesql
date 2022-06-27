mod blend;
mod error;

pub use error::SelectError;

use {
    self::blend::Blend,
    super::{
        aggregate::Aggregator,
        context::{BlendContext, FilterContext},
        fetch::{
            fetch_columns, fetch_join_columns, fetch_name, get_alias, get_index, get_name, Rows,
        },
        filter::Filter,
        join::Join,
        limit::Limit,
        sort::Sort,
    },
    crate::{
        ast::{Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins},
        data::Row,
        result::{Error, Result},
        store::GStore,
    },
    async_recursion::async_recursion,
    futures::stream::{self, Stream, TryStream, TryStreamExt},
    iter_enum::Iterator,
    std::{iter::once, rc::Rc},
};

#[cfg(feature = "index")]
use {super::evaluate::evaluate, crate::ast::IndexItem};

async fn fetch_blended<'a>(
    storage: &'a dyn GStore,
    relation: &'a TableFactor,
    columns: Rc<[String]>,
) -> Result<impl Stream<Item = Result<BlendContext<'a>>> + 'a> {
    let table_name = get_name(relation)?;

    #[cfg(feature = "index")]
    let rows = {
        #[derive(Iterator)]
        enum Rows<I1, I2> {
            FullScan(I1),
            Indexed(I2),
        }

        match get_index(relation) {
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

    let rows = rows.map(move |data| {
        let (_, row) = data?;
        let row = Some(row);
        let columns = Rc::clone(&columns);

        Ok(BlendContext::new(get_alias(relation)?, columns, row, None))
    });
    Ok(stream::iter(rows))
}

pub fn get_labels<'a>(
    projection: &[SelectItem],
    table_alias: &str,
    columns: &'a [String],
    join_columns: Option<&'a [(&String, Vec<String>)]>,
) -> Result<Vec<String>> {
    #[derive(Iterator)]
    enum Labeled<I1, I2, I3, I4, I5> {
        Err(I1),
        Wildcard(Wildcard<I2, I3>),
        QualifiedWildcard(I4),
        Once(I5),
    }

    #[derive(Iterator)]
    enum Wildcard<I2, I3> {
        WithJoin(I2),
        WithoutJoin(I3),
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
                if let Some(join_columns) = join_columns {
                    let join_labels = join_columns
                        .iter()
                        .flat_map(|(_, columns)| to_labels(columns));
                    let labels = labels.chain(join_labels).map(Ok);
                    return Labeled::Wildcard(Wildcard::WithJoin(labels));
                };
                let labels = labels.map(Ok);
                Labeled::Wildcard(Wildcard::WithoutJoin(labels))
            }
            SelectItem::QualifiedWildcard(target) => {
                let target_table_alias = try_into!(fetch_name(target));

                if table_alias == target_table_alias {
                    return Labeled::QualifiedWildcard(to_labels(columns).map(Ok));
                }

                if let Some(join_columns) = join_columns {
                    let columns = join_columns
                        .iter()
                        .find(|(table_alias, _)| table_alias == &target_table_alias)
                        .map(|(_, columns)| columns)
                        .ok_or_else(|| {
                            SelectError::TableAliasNotFound(target_table_alias.to_string()).into()
                        });
                    let columns = try_into!(columns);
                    let labels = to_labels(columns);
                    return Labeled::QualifiedWildcard(labels.map(Ok));
                }
                let labels = to_labels(&[]).map(Ok);
                Labeled::QualifiedWildcard(labels)
            }
            SelectItem::Expr { label, .. } => Labeled::Once(once(Ok(label.to_owned()))),
        })
        .collect::<Result<_>>()
}

#[async_recursion(?Send)]
pub async fn select_with_labels<'a>(
    storage: &'a dyn GStore,
    query: &'a Query,
    filter_context: Option<Rc<FilterContext<'a>>>,
    with_labels: bool,
) -> Result<(
    Vec<String>,
    impl TryStream<Ok = Row, Error = Error, Item = Result<Row>> + 'a,
)> {
    let Select {
        from: table_with_joins,
        selection: where_clause,
        projection,
        group_by,
        having,
        order_by,
    } = match &query.body {
        SetExpr::Select(statement) => statement.as_ref(),
        _ => {
            return Err(SelectError::Unreachable.into());
        }
    };

    let TableWithJoins { relation, joins } = &table_with_joins;
    let (rows, columns) = match relation {
        TableFactor::Table { .. } => {
            let columns = fetch_columns(storage, get_name(relation)?).await?;
            let columns = Rc::from(columns);
            (
                Rows::Table(fetch_blended(storage, relation, Rc::clone(&columns)).await?),
                columns,
            )
        }
        TableFactor::Derived { subquery, alias } => {
            let (labels, inline_view) = select_with_labels(storage, subquery, None, true).await?;
            let inline_view = inline_view.try_collect::<Vec<_>>().await?;
            let labels_rc = Rc::from(labels.to_owned());
            let labels = Rc::clone(&labels_rc);
            let rows = inline_view.into_iter().map(move |row| {
                Ok(BlendContext::new(
                    &alias.name,
                    Rc::clone(&labels_rc),
                    Some(row),
                    None,
                ))
            });
            (Rows::Derived(stream::iter(rows)), Rc::clone(&labels))
        }
    };

    let join_columns = fetch_join_columns(joins, storage).await?;

    let labels = if with_labels {
        get_labels(
            projection,
            get_alias(relation)?,
            &columns,
            Some(&join_columns),
        )?
    } else {
        vec![]
    };

    let join_columns = join_columns
        .into_iter()
        .map(|(_, columns)| columns)
        .map(Rc::from)
        .collect::<Vec<_>>();
    let join = Join::new(
        storage,
        joins,
        join_columns,
        filter_context.as_ref().map(Rc::clone),
    );

    let aggregate = Aggregator::new(
        storage,
        projection,
        group_by,
        having.as_ref(),
        filter_context.as_ref().map(Rc::clone),
    );
    let blend = Rc::new(Blend::new(
        storage,
        filter_context.as_ref().map(Rc::clone),
        projection,
    ));
    let filter = Rc::new(Filter::new(
        storage,
        where_clause.as_ref(),
        filter_context.as_ref().map(Rc::clone),
        None,
    ));
    let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref())?;
    let sort = Sort::new(storage, filter_context, order_by);

    let rows = join.apply(rows).await?;
    let rows = rows.try_filter_map(move |blend_context| {
        let filter = Rc::clone(&filter);

        async move {
            filter
                .check(Rc::clone(&blend_context))
                .await
                .map(|pass| pass.then(|| blend_context))
        }
    });

    let rows = aggregate.apply(rows).await?;
    let rows = sort
        .apply(rows)
        .await?
        .and_then(move |(aggregated, context)| {
            let blend = Rc::clone(&blend);

            async move { blend.apply(aggregated, context).await }
        });
    let rows = limit.apply(rows);

    Ok((labels, rows))
}

pub async fn select<'a>(
    storage: &'a dyn GStore,
    query: &'a Query,
    filter_context: Option<Rc<FilterContext<'a>>>,
) -> Result<impl TryStream<Ok = Row, Error = Error, Item = Result<Row>> + 'a> {
    select_with_labels(storage, query, filter_context, false)
        .await
        .map(|(_, rows)| rows)
}
