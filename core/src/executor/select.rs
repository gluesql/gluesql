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
        sort::{Sort, SortError},
    },
    crate::{
        ast::{Literal, UnaryOperator},
        data::{Key, Row, Value},
        plan::{
            ExprPlan, OrderByExprPlan, QueryPlan, SelectPlan, SetExprPlan, TableWithJoinsPlan,
            ValuesPlan,
        },
        result::Result,
        store::GStore,
    },
    async_recursion::async_recursion,
    async_stream::try_stream,
    bigdecimal::ToPrimitive,
    futures::stream::{self, StreamExt, TryStreamExt},
    hashbrown::hash_map::RawEntryMut,
    std::{borrow::Cow, collections::HashSet, pin::Pin, sync::Arc},
    utils::Vector,
};

fn apply_distinct(rows: Vec<Row>) -> Vec<Row> {
    let mut seen = HashSet::new();

    rows.into_iter()
        .filter(|row| seen.insert(row.values.clone()))
        .collect()
}

async fn rows_with_labels(exprs_list: &[Vec<ExprPlan>]) -> Result<(Vec<Row>, Vec<String>)> {
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

fn positional_index(expr: &ExprPlan) -> Option<&bigdecimal::BigDecimal> {
    match expr {
        ExprPlan::Literal(Literal::Number(n)) => Some(n),
        ExprPlan::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => match expr.as_ref() {
            ExprPlan::Literal(Literal::Number(n)) => Some(n),
            _ => None,
        },
        _ => None,
    }
}

async fn sort_stateless(rows: Vec<Row>, order_by: &[OrderByExprPlan]) -> Result<Vec<Row>> {
    let sorted = stream::iter(rows.into_iter())
        .then(|row| async move {
            stream::iter(order_by)
                .then(|OrderByExprPlan { expr, asc }| {
                    let row = Some(&row);

                    async move {
                        // Positional column index: ORDER BY 1 refers to the first
                        // output column, not the constant integer 1.
                        if let Some(n) = positional_index(expr) {
                            let index = n.to_usize().ok_or_else(|| -> crate::result::Error {
                                SortError::Unreachable.into()
                            })?;
                            let zero_based =
                                index
                                    .checked_sub(1)
                                    .ok_or_else(|| -> crate::result::Error {
                                        SortError::ColumnIndexOutOfRange(index).into()
                                    })?;
                            let value = row
                                .and_then(|r| r.values.get(zero_based).cloned())
                                .ok_or_else(|| -> crate::result::Error {
                                    SortError::ColumnIndexOutOfRange(index).into()
                                })?;
                            return Key::try_from(value).map(|key| (key, *asc));
                        }

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

#[async_recursion]
pub async fn select_with_labels<'a, T>(
    storage: &'a T,
    query: Arc<QueryPlan>,
    filter_context: Option<Arc<RowContext<'a>>>,
) -> Result<(
    Vec<String>,
    Pin<Box<dyn futures::stream::Stream<Item = Result<Row>> + Send + 'a>>,
)>
where
    T: GStore,
{
    match query.body.clone() {
        SetExprPlan::Union { left, right, all } => {
            let left_query = Arc::new(QueryPlan::from(*left));
            let right_query = Arc::new(QueryPlan::from(*right));
            select_union(storage, query, left_query, right_query, all, filter_context).await
        }
        SetExprPlan::Values(ValuesPlan(values_list)) => {
            let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref()).await?;
            let (rows, labels) = rows_with_labels(&values_list).await?;
            let rows = sort_stateless(rows, &query.order_by).await?;
            let rows = stream::iter(rows.into_iter().map(Ok));
            let stream: Pin<Box<dyn futures::stream::Stream<Item = Result<Row>> + Send + 'a>> =
                Box::pin(limit.apply(rows));
            Ok((labels, stream))
        }
        SetExprPlan::Select(select) => select_from(storage, select, query, filter_context).await,
    }
}

async fn select_from<'a, T>(
    storage: &'a T,
    select: Box<SelectPlan>,
    query: Arc<QueryPlan>,
    filter_context: Option<Arc<RowContext<'a>>>,
) -> Result<(
    Vec<String>,
    Pin<Box<dyn futures::stream::Stream<Item = Result<Row>> + Send + 'a>>,
)>
where
    T: GStore,
{
    let labels = {
        let TableWithJoinsPlan { relation, joins } = &select.from;
        fetch_labels(storage, relation, joins, &select.projection).await?
    };
    let labels_clone = labels.clone();

    let stream = try_stream! {
        let SelectPlan {
            distinct,
            from: table_with_joins,
            selection: where_clause,
            projection,
            group_by,
            having,
            aggregate_slots,
        } = select.as_ref();
        let TableWithJoinsPlan { relation, joins } = table_with_joins;

        let rows = fetch_relation_rows(storage, relation, None).await?;
        let rows = rows.map(move |row| {
            let row = row?;
            let alias = relation.alias_name();
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

        let rows = sort.apply(rows, relation.alias_name()).await?;

        if *distinct {
            let mut seen: hashbrown::HashMap<Vec<Value>, ()> = hashbrown::HashMap::new();
            let rows = rows.try_filter_map(move |row: Row| {
                match seen.raw_entry_mut().from_key(&row.values) {
                    RawEntryMut::Occupied(_) => std::future::ready(Ok(None)),
                    RawEntryMut::Vacant(e) => {
                        e.insert(row.values.clone(), ());
                        std::future::ready(Ok(Some(row)))
                    }
                }
            });
            let limited = limit.apply(rows);
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

async fn select_union<'a, T>(
    storage: &'a T,
    outer: Arc<QueryPlan>,
    left_query: Arc<QueryPlan>,
    right_query: Arc<QueryPlan>,
    all: bool,
    filter_context: Option<Arc<RowContext<'a>>>,
) -> Result<(
    Vec<String>,
    Pin<Box<dyn futures::stream::Stream<Item = Result<Row>> + Send + 'a>>,
)>
where
    T: GStore,
{
    let (labels, left_stream) =
        select_with_labels(storage, left_query, filter_context.as_ref().map(Arc::clone)).await?;
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

    let left_relabeled = left_stream.map_ok({
        let la = Arc::clone(&labels_arc);
        move |row| Row {
            columns: Arc::clone(&la),
            values: row.values,
        }
    });
    let right_relabeled = right_stream.map_ok(move |row| Row {
        columns: Arc::clone(&labels_arc),
        values: row.values,
    });

    // Without ORDER BY, both UNION ALL and UNION DISTINCT can stream lazily.
    if outer.order_by.is_empty() {
        let combined = left_relabeled.chain(right_relabeled);

        let stream: Pin<Box<dyn futures::stream::Stream<Item = Result<Row>> + Send + 'a>> = if all {
            Box::pin(combined)
        } else {
            let mut seen: hashbrown::HashMap<Vec<Value>, ()> = hashbrown::HashMap::new();
            Box::pin(combined.try_filter_map(move |row: Row| {
                match seen.raw_entry_mut().from_key(&row.values) {
                    RawEntryMut::Occupied(_) => std::future::ready(Ok(None)),
                    RawEntryMut::Vacant(e) => {
                        e.insert(row.values.clone(), ());
                        std::future::ready(Ok(Some(row)))
                    }
                }
            }))
        };

        let limit = Limit::new(outer.limit.as_ref(), outer.offset.as_ref()).await?;
        return Ok((labels, Box::pin(limit.apply(stream))));
    }

    // ORDER BY present: materialise then sort.
    let mut rows: Vec<Row> = left_relabeled.try_collect().await?;
    rows.extend(right_relabeled.try_collect::<Vec<_>>().await?);

    if !all {
        rows = apply_distinct(rows);
    }

    let rows = sort_stateless(rows, &outer.order_by).await?;
    let limit = Limit::new(outer.limit.as_ref(), outer.offset.as_ref()).await?;
    let rows = stream::iter(rows.into_iter().map(Ok));
    Ok((labels, Box::pin(limit.apply(rows))))
}

pub async fn select<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Arc<RowContext<'a>>>,
) -> Result<impl futures::stream::Stream<Item = Result<Row>> + Send + 'a>
where
    T: GStore,
{
    select_with_labels(storage, Arc::new(query.clone()), filter_context)
        .await
        .map(|(_, rows)| rows)
}
