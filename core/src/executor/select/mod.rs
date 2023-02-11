mod error;
mod project;

pub use error::SelectError;

use {
    self::project::Project,
    super::{
        aggregate::Aggregator,
        context::{AggregateContext, RowContext},
        evaluate_stateless,
        fetch::{fetch_labels, fetch_relation_rows},
        filter::Filter,
        join::Join,
        limit::Limit,
        sort::Sort,
    },
    crate::{
        ast::{Expr, OrderByExpr, Query, Select, SetExpr, TableWithJoins, Values},
        data::{get_alias, Row},
        prelude::{DataType, Value},
        result::{Error, Result},
        store::GStore,
    },
    futures::{
        future::LocalBoxFuture,
        stream::{self, StreamExt, TryStream, TryStreamExt},
    },
    std::{borrow::Cow, iter, rc::Rc},
    utils::Vector,
};

fn rows_with_labels(exprs_list: &[Vec<Expr>]) -> (Vec<Result<Row>>, Vec<String>) {
    let first_len = exprs_list[0].len();
    let labels = (1..=first_len)
        .into_iter()
        .map(|i| format!("column{}", i))
        .collect::<Vec<_>>();

    let columns = Rc::from(labels.clone());
    let rows = exprs_list
        .iter()
        .scan(
            iter::repeat(None)
                .take(first_len)
                .collect::<Vec<Option<DataType>>>(),
            move |column_types, exprs| {
                if exprs.len() != first_len {
                    return Some(Err(SelectError::NumberOfValuesDifferent.into()));
                }

                let values = column_types
                    .iter_mut()
                    .zip(exprs.iter())
                    .map(|(column_type, expr)| -> Result<_> {
                        let evaluated = evaluate_stateless(None, expr)?;

                        let value = match column_type {
                            Some(data_type) => evaluated.try_into_value(data_type, true)?,
                            None => {
                                let value: Value = evaluated.try_into()?;
                                *column_type = value.get_type();

                                value
                            }
                        };

                        Ok(value)
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(|values| Row::Vec {
                        columns: Rc::clone(&columns),
                        values,
                    });

                Some(values)
            },
        )
        .collect::<Vec<_>>();

    (rows, labels)
}

fn sort_stateless(rows: Vec<Result<Row>>, order_by: &[OrderByExpr]) -> Result<Vec<Result<Row>>> {
    let sorted = rows
        .into_iter()
        .map(|row| {
            let values = order_by
                .iter()
                .map(|OrderByExpr { expr, asc }| -> Result<_> {
                    let row = row.as_ref().ok();
                    let value: Value = evaluate_stateless(row, expr)?.try_into()?;

                    Ok((value, *asc))
                })
                .collect::<Result<Vec<_>>>();

            values.map(|values| (values, row))
        })
        .collect::<Result<Vec<_>>>()
        .map(Vector::from)?
        .sort_by(|(values_a, _), (values_b, _)| super::sort::sort_by(values_a, values_b))
        .into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>();

    Ok(sorted)
}

pub fn select_with_labels<'storage, 'context, T: GStore>(
    storage: &'storage T,
    query: &'storage Query,
    filter_context: Option<Rc<RowContext<'context>>>,
) -> LocalBoxFuture<
    'storage,
    Result<(
        Option<Vec<String>>,
        impl TryStream<Ok = Row, Error = Error, Item = Result<Row>> + 'storage,
    )>,
>
where
    'context: 'storage,
{
    Box::pin(select_with_labels_inner(storage, query, filter_context))
}

async fn select_with_labels_inner<'storage, 'context, T: GStore>(
    storage: &'storage T,
    query: &'storage Query,
    filter_context: Option<Rc<RowContext<'context>>>,
) -> Result<(
    Option<Vec<String>>,
    impl TryStream<Ok = Row, Error = Error, Item = Result<Row>> + 'storage,
)>
where
    'context: 'storage,
{
    let Select {
        from: table_with_joins,
        selection: where_clause,
        projection,
        group_by,
        having,
    } = match &query.body {
        SetExpr::Select(statement) => statement.as_ref(),
        SetExpr::Values(Values(values_list)) => {
            let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref())?;
            let (rows, labels) = rows_with_labels(values_list);
            let rows = sort_stateless(rows, &query.order_by)?;
            let rows = stream::iter(rows);
            let rows = limit.apply(rows);

            return Ok((Some(labels), rows));
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
    let limit = Limit::new(query.limit.as_ref(), query.offset.as_ref())?;
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

    Ok((labels, rows))
}

pub async fn select<'a, T: GStore>(
    storage: &'a T,
    query: &'a Query,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<impl TryStream<Ok = Row, Error = Error, Item = Result<Row>> + 'a> {
    select_with_labels(storage, query, filter_context)
        .await
        .map(|(_, rows)| rows)
}
