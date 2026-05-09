mod error;
mod project;

pub use error::SelectError;
use {
    self::project::Project,
    super::{
        aggregate,
        context::{AggregateContext, RowContext},
        evaluate::evaluate_stateless,
        fetch::{fetch_labels, fetch_relation_rows},
        filter::Filter,
        join::Join,
        limit::Limit,
        sort::Sort,
    },
    crate::{
        ast::{Expr, OrderByExpr, Query, Select, SetExpr, TableWithJoins, Values},
        data::{Key, Row, Value, get_alias},
        result::Result,
        store::GStore,
    },
    async_recursion::async_recursion,
    async_stream::try_stream,
    futures::stream::{self, StreamExt, TryStreamExt},
    std::{borrow::Cow, collections::HashSet, pin::Pin, sync::Arc},
    utils::Vector,
};

fn apply_distinct(rows: Vec<Row>) -> Vec<Row> {
    let mut seen = HashSet::new();

    rows.into_iter()
        .filter(|row| seen.insert(row.values.clone()))
        .collect()
}

async fn rows_with_labels(exprs_list: &[Vec<Expr>]) -> Result<(Vec<Row>, Vec<String>)> {
    let first_len = exprs_list[0].len();
    let labels = (1..=first_len)
        .map(|i| format!("column{i}"))
        .collect::<Vec<_>>();
    let columns = Arc::from(labels.clone());

    let mut column_types = vec![None; first_len];
    let mut rows = Vec::with_capacity(exprs_list.len());

    for exprs in exprs_list {
        if exprs.len() != first_len {
            return Err(SelectError::NumberOfValuesDifferent.into());
        }

        let mut values = Vec::with_capacity(exprs.len());

        for (expr, column_type) in exprs.iter().zip(column_types.iter_mut()) {
            let evaluated = evaluate_stateless(None, expr).await?;

            let value = if let Some(data_type) = column_type.as_ref() {
                evaluated.try_into_value(data_type, true)?
            } else {
                let value: Value = evaluated.try_into()?;
                *column_type = value.get_type();
                value
            };

            values.push(value);
        }

        rows.push(Row {
            columns: Arc::clone(&columns),
            values,
        });
    }

    Ok((rows, labels))
}

async fn sort_stateless(rows: Vec<Row>, order_by: &[OrderByExpr]) -> Result<Vec<Row>> {
    let sorted = stream::iter(rows.into_iter())
        .then(|row| async move {
            stream::iter(order_by)
                .then(|OrderByExpr { expr, asc }| {
                    let row = Some(&row);

                    async move {
                        evaluate_stateless(row.map(Row::as_context), expr)
                            .await
                            .and_then(Value::try_from)
                            .and_then(Key::try_from)
                            .map(|key| (key, *asc))
                    }
                })
                .try_collect::<Vec<_>>()
                .await
                .map(|keys| (keys, row))
        })
        .try_collect::<Vec<_>>()
        .await
        .map(Vector::from)?
        .sort_by(|(keys_a, _), (keys_b, _)| super::sort::sort_by(keys_a, keys_b))
        .into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>();

    Ok(sorted)
}

/// Entry point for executing a SELECT/UNION/VALUES query and returning the
/// column labels alongside a lazy stream of result rows.
///
/// `query` is wrapped in [`Arc`] so that recursive UNION branches can each
/// hold an independent owning reference to their sub-query, allowing the
/// returned stream to keep the query data alive without borrowing from a
/// shorter-lived stack frame.  This is what enables true streaming for
/// `UNION ALL` queries that carry no outer `ORDER BY` / `LIMIT` / `OFFSET`:
/// the two branch streams are chained and yielded lazily instead of being
/// fully materialised before the first row reaches the caller.
#[async_recursion]
pub async fn select_with_labels<'a, T>(
    storage: &'a T,
    query: Arc<Query>,
    filter_context: Option<Arc<RowContext<'a>>>,
) -> Result<(
    Vec<String>,
    Pin<Box<dyn futures::stream::Stream<Item = Result<Row>> + Send + 'a>>,
)>
where
    T: GStore,
{
    // ── UNION branch ─────────────────────────────────────────────────────────
    if let SetExpr::Union { left, right, all } = &query.body {
        let all = *all;
        let left_query = Arc::new(Query {
            body: *left.clone(),
            order_by: vec![],
            limit: None,
            offset: None,
        });
        let right_query = Arc::new(Query {
            body: *right.clone(),
            order_by: vec![],
            limit: None,
            offset: None,
        });

        let (labels, left_stream) =
            select_with_labels(storage, left_query, filter_context.as_ref().map(Arc::clone))
                .await?;
        let (right_labels, right_stream) = select_with_labels(
            storage,
            right_query,
            filter_context.as_ref().map(Arc::clone),
        )
        .await?;

        if labels.len() != right_labels.len() {
            return Err(SelectError::UnionColumnCountMismatch {
                left: labels.len(),
                right: right_labels.len(),
            }
            .into());
        }

        let labels_arc: Arc<[String]> = Arc::from(labels.as_slice());

        // UNION ALL with no outer ORDER BY / LIMIT / OFFSET: chain streams
        // lazily — rows flow to the caller without full materialisation.
        if all && query.order_by.is_empty() && query.limit.is_none() && query.offset.is_none() {
            let left_relabeled = left_stream.map_ok({
                let labels_arc = Arc::clone(&labels_arc);
                move |row| Row {
                    columns: Arc::clone(&labels_arc),
                    values: row.values,
                }
            });
            let right_relabeled = right_stream.map_ok(move |row| Row {
                columns: Arc::clone(&labels_arc),
                values: row.values,
            });
            let chained: Pin<Box<dyn futures::stream::Stream<Item = Result<Row>> + Send + 'a>> =
                Box::pin(left_relabeled.chain(right_relabeled));
            return Ok((labels, chained));
        }

        // Materialise for UNION DISTINCT or when ORDER BY / LIMIT is present.
        let mut rows: Vec<Row> = left_stream
            .map_ok(|row| Row {
                columns: Arc::clone(&labels_arc),
                values: row.values,
            })
            .try_collect()
            .await?;
        let right_rows: Vec<Row> = right_stream
            .map_ok(|row| Row {
                columns: Arc::clone(&labels_arc),
                values: row.values,
            })
            .try_collect()
            .await?;
        rows.extend(right_rows);

        if !all {
            rows = apply_distinct(rows);
        }

        let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref()).await?;
        let rows = if query.order_by.is_empty() {
            rows
        } else {
            sort_stateless(rows, &query.order_by).await?
        };
        let rows = stream::iter(rows.into_iter().map(Ok));
        let rows = limit.apply(rows);
        return Ok((labels, Box::pin(rows)));
    }

    // ── VALUES branch ────────────────────────────────────────────────────────
    if let SetExpr::Values(Values(values_list)) = &query.body {
        let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref()).await?;
        let (rows, labels) = rows_with_labels(values_list).await?;
        let rows = sort_stateless(rows, &query.order_by).await?;
        let rows = stream::iter(rows.into_iter().map(Ok));
        let rows = limit.apply(rows);
        return Ok((labels, Box::pin(rows)));
    }

    // ── SELECT branch ────────────────────────────────────────────────────────
    // Compute labels eagerly (needs refs into `query`) before moving `query`
    // into the stream generator below.
    let labels = {
        let select = match &query.body {
            SetExpr::Select(s) => s.as_ref(),
            _ => unreachable!(),
        };
        let TableWithJoins { relation, joins } = &select.from;
        fetch_labels(storage, relation, joins, &select.projection).await?
    };
    let labels_clone = labels.clone();

    // Build the row stream inside `try_stream!` so that `query: Arc<Query>`
    // is captured by move into the generator state machine.  This keeps all
    // query data alive for the entire lifetime of the stream without borrowing
    // from any stack frame.
    let stream = try_stream! {
        let select = match &query.body {
            SetExpr::Select(s) => s.as_ref(),
            _ => unreachable!(),
        };
        let Select {
            distinct,
            from: table_with_joins,
            selection: where_clause,
            projection,
            group_by,
            having,
            aggregate_slots,
        } = select;
        let TableWithJoins { relation, joins } = table_with_joins;

        let rows = fetch_relation_rows(storage, relation, None).await?;
        let rows = rows.map(|row| {
            let row = row?;
            let alias = get_alias(relation);
            Ok(RowContext::new(alias, Cow::Owned(row), None))
        });

        let join = Join::new(storage, joins, filter_context.as_ref().map(Arc::clone));
        let filter = Arc::new(Filter::new(
            storage,
            where_clause.as_ref(),
            filter_context.as_ref().map(Arc::clone),
        ));
        let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref()).await?;
        let sort = Sort::new(
            storage,
            filter_context.as_ref().map(Arc::clone),
            &query.order_by,
        );

        let rows = join.apply(rows).await?;
        let rows = rows.try_filter_map(move |project_context| {
            let filter = Arc::clone(&filter);
            async move {
                filter
                    .check(Arc::clone(&project_context))
                    .await
                    .map(|pass| pass.then_some(project_context))
            }
        });

        let rows = aggregate::apply(
            storage,
            aggregate_slots.as_deref(),
            group_by,
            having.as_ref(),
            filter_context.as_ref().map(Arc::clone),
            rows,
        )
        .await?;

        let labels_arc: Arc<[String]> = Arc::from(labels_clone.as_slice());
        let project = Arc::new(Project::new(storage, filter_context, projection));
        let project_labels = Arc::clone(&labels_arc);
        let rows = rows.and_then(move |aggregate_context| {
            let labels = Arc::clone(&project_labels);
            let project = Arc::clone(&project);
            let AggregateContext { aggregated, next } = aggregate_context;
            async move {
                let row = project
                    .apply(
                        aggregated.as_ref().map(Arc::clone),
                        labels,
                        next.as_ref().map(Arc::clone),
                    )
                    .await?;
                Ok((aggregated, next, row))
            }
        });

        let rows = sort.apply(rows, get_alias(relation)).await?;

        if *distinct {
            let all_rows: Vec<Row> = rows.try_collect().await?;
            let unique_rows = apply_distinct(all_rows);
            let limited = limit.apply(stream::iter(unique_rows.into_iter().map(Ok)));
            futures::pin_mut!(limited);
            while let Some(item) = limited.next().await {
                yield item?;
            }
        } else {
            let limited = limit.apply(rows);
            futures::pin_mut!(limited);
            while let Some(item) = limited.next().await {
                yield item?;
            }
        }
    };

    Ok((labels, Box::pin(stream)))
}

pub async fn select<'a, T>(
    storage: &'a T,
    query: &'a Query,
    filter_context: Option<Arc<RowContext<'a>>>,
) -> Result<impl futures::stream::Stream<Item = Result<Row>> + Send + 'a>
where
    T: GStore,
{
    select_with_labels(storage, Arc::new(query.clone()), filter_context)
        .await
        .map(|(_, rows)| rows)
}
