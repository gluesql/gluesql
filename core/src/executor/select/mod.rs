mod error;
mod project;

pub use error::SelectError;

use {
    self::project::Project,
    super::{
        aggregate::Aggregator,
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
        data::{get_alias, Key, Row, Value},
        result::Result,
        store::GStore,
    },
    async_recursion::async_recursion,
    futures::stream::{self, Stream, StreamExt, TryStreamExt},
    std::{borrow::Cow, rc::Rc},
    utils::Vector,
};

async fn rows_with_labels(exprs_list: &[Vec<Expr>]) -> Result<(Vec<Row>, Vec<String>)> {
    let first_len = exprs_list[0].len();
    let labels = (1..=first_len)
        .map(|i| format!("column{}", i))
        .collect::<Vec<_>>();
    let columns = Rc::from(labels.clone());

    let mut column_types = vec![None; first_len];
    let mut rows = Vec::with_capacity(exprs_list.len());

    for exprs in exprs_list {
        if exprs.len() != first_len {
            return Err(SelectError::NumberOfValuesDifferent.into());
        }

        let mut values = Vec::with_capacity(exprs.len());

        for (i, expr) in exprs.iter().enumerate() {
            let evaluated = evaluate_stateless(None, expr).await?;

            let value = match column_types[i] {
                Some(ref data_type) => evaluated.try_into_value(data_type, true)?,
                None => {
                    let value: Value = evaluated.try_into()?;
                    column_types[i] = value.get_type();

                    value
                }
            };

            values.push(value);
        }

        rows.push(Row::Vec {
            columns: Rc::clone(&columns),
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

#[async_recursion(?Send)]
pub async fn select_with_labels<'a, T>(
    storage: &'a T,
    query: &'a Query,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<(Option<Vec<String>>, impl Stream<Item = Result<Row>> + 'a)>
where
    T: GStore,
{
    #[derive(futures_enum::Stream)]
    enum Row<S1, S2> {
        Select(S2),
        Values(S1),
    }

    let Select {
        from: table_with_joins,
        selection: where_clause,
        projection,
        group_by,
        having,
    } = match &query.body {
        SetExpr::Select(statement) => statement.as_ref(),
        SetExpr::Values(Values(values_list)) => {
            let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref()).await?;
            let (rows, labels) = rows_with_labels(values_list).await?;
            let rows = sort_stateless(rows, &query.order_by).await?;
            let rows = stream::iter(rows.into_iter().map(Ok));
            let rows = limit.apply(rows);

            return Ok((Some(labels), Row::Values(rows)));
        }
    };

    let TableWithJoins { relation, joins } = &table_with_joins;
    let rows = fetch_relation_rows(storage, relation, &None)
        .await?
        .map(move |row| {
            let row = row?;
            let alias = get_alias(relation);

            Ok(RowContext::new(alias, Cow::Owned(row), None))
        });

    let join = Join::new(storage, joins, filter_context.as_ref().map(Rc::clone));
    let aggregate = Aggregator::new(
        storage,
        projection,
        group_by,
        having.as_ref(),
        filter_context.as_ref().map(Rc::clone),
    );
    let filter = Rc::new(Filter::new(
        storage,
        where_clause.as_ref(),
        filter_context.as_ref().map(Rc::clone),
        None,
    ));
    let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref()).await?;
    let sort = Sort::new(
        storage,
        filter_context.as_ref().map(Rc::clone),
        &query.order_by,
    );

    let rows = join.apply(rows).await?;
    let rows = rows.try_filter_map(move |project_context| {
        let filter = Rc::clone(&filter);

        async move {
            filter
                .check(Rc::clone(&project_context))
                .await
                .map(|pass| pass.then_some(project_context))
        }
    });

    let rows = aggregate.apply(rows).await?;

    let labels = fetch_labels(storage, relation, joins, projection)
        .await?
        .map(Rc::from);

    let project = Rc::new(Project::new(storage, filter_context, projection));
    let project_labels = labels.as_ref().map(Rc::clone);
    let rows = rows.and_then(move |aggregate_context| {
        let labels = project_labels.as_ref().map(Rc::clone);
        let project = Rc::clone(&project);
        let AggregateContext { aggregated, next } = aggregate_context;
        let aggregated = aggregated.map(Rc::new);

        async move {
            let row = project
                .apply(aggregated.as_ref().map(Rc::clone), labels, Rc::clone(&next))
                .await?;

            Ok((aggregated, next, row))
        }
    });

    let rows = sort.apply(rows, get_alias(relation)).await?;
    let rows = limit.apply(rows);
    let labels = labels.map(|labels| labels.iter().cloned().collect());

    Ok((labels, Row::Select(rows)))
}

pub async fn select<'a, T: GStore>(
    storage: &'a T,
    query: &'a Query,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<impl Stream<Item = Result<Row>> + 'a> {
    select_with_labels(storage, query, filter_context)
        .await
        .map(|(_, rows)| rows)
}
