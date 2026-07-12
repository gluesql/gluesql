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
        limit,
        sort::Sort,
    },
    crate::{
        data::{Key, Row, Value},
        plan::{
            ExprPlan, OrderByExprPlan, QueryPlan, SelectPlan, SetExprPlan, TableWithJoinsPlan,
            ValuesPlan,
        },
        result::Result,
        store::GStore,
    },
    std::{borrow::Cow, collections::HashSet, rc::Rc},
};

pub type SelectIter<'a> = Box<dyn Iterator<Item = Result<Row>> + 'a>;

fn apply_distinct(rows: Vec<Row>) -> Vec<Row> {
    let mut seen = HashSet::new();

    rows.into_iter()
        .filter(|row| seen.insert(row.values.clone()))
        .collect()
}

fn rows_with_labels(exprs_list: &[Vec<ExprPlan>]) -> Result<(Vec<Row>, Vec<String>)> {
    let first_len = exprs_list[0].len();
    let labels = (1..=first_len)
        .map(|i| format!("column{i}"))
        .collect::<Vec<_>>();
    let columns = Rc::from(labels.clone());

    let mut column_types = vec![None; first_len];
    let mut rows = Vec::with_capacity(exprs_list.len());

    for exprs in exprs_list {
        if exprs.len() != first_len {
            return Err(SelectError::NumberOfValuesDifferent.into());
        }

        let mut values = Vec::with_capacity(exprs.len());

        for (expr, column_type) in exprs.iter().zip(column_types.iter_mut()) {
            let evaluated = evaluate_stateless(None, expr)?;

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
            columns: Rc::clone(&columns),
            values,
        });
    }

    Ok((rows, labels))
}

fn sort_stateless(rows: Vec<Row>, order_by: &[OrderByExprPlan]) -> Result<Vec<Row>> {
    let mut keyed_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let keys = order_by
            .iter()
            .map(|OrderByExprPlan { expr, asc }| {
                evaluate_stateless(Some(row.as_context()), expr)
                    .and_then(Value::try_from)
                    .and_then(Key::try_from)
                    .map(|key| (key, *asc))
            })
            .collect::<Result<Vec<_>>>()?;

        keyed_rows.push((keys, row));
    }

    keyed_rows.sort_by(|(keys_a, _), (keys_b, _)| super::sort::sort_by(keys_a, keys_b));

    let sorted = keyed_rows
        .into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>();

    Ok(sorted)
}

pub fn select_with_labels<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<(Vec<String>, SelectIter<'a>)>
where
    T: GStore,
{
    let SelectPlan {
        distinct,
        from: table_with_joins,
        selection: where_clause,
        projection,
        group_by,
        having,
        aggregate_slots,
    } = match query.body() {
        SetExprPlan::Select(statement) => statement.as_ref(),
        SetExprPlan::Values(ValuesPlan(values_list)) => {
            let (rows, labels) = rows_with_labels(values_list)?;
            let rows = sort_stateless(rows, query.order_by())?;
            let rows = limit::apply(query, rows.into_iter().map(Ok))?;

            return Ok((labels, rows));
        }
    };

    let TableWithJoinsPlan { relation, joins } = &table_with_joins;
    let rows = fetch_relation_rows(storage, relation, None)?.map(move |row| {
        let row = row?;
        let alias = relation.alias_name();

        Ok(RowContext::new(alias, Cow::Owned(row), None))
    });

    let join = Join::new(storage, joins, filter_context.as_ref().map(Rc::clone));
    let filter = Rc::new(Filter::new(
        storage,
        where_clause.as_ref(),
        filter_context.as_ref().map(Rc::clone),
    ));
    let sort = Sort::new(
        storage,
        filter_context.as_ref().map(Rc::clone),
        query.order_by(),
    );

    let rows = join.apply(Box::new(rows))?;
    let rows = rows.filter_map(move |project_context| {
        let project_context = match project_context {
            Ok(project_context) => project_context,
            Err(error) => return Some(Err(error)),
        };

        match filter.check(Rc::clone(&project_context)) {
            Ok(true) => Some(Ok(project_context)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        }
    });

    let rows = aggregate::apply(
        storage,
        aggregate_slots.as_deref(),
        group_by,
        having.as_ref(),
        filter_context.as_ref(),
        Box::new(rows),
    )?;

    let labels = fetch_labels(storage, relation, joins, projection)?;
    let labels = Rc::from(labels);
    let project = Rc::new(Project::new(storage, filter_context, projection));
    let project_labels = Rc::clone(&labels);
    let rows = rows.map(move |aggregate_context| {
        let aggregate_context = aggregate_context?;
        let project = Rc::clone(&project);
        let AggregateContext { aggregated, next } = aggregate_context;

        let row = project.apply(aggregated.as_ref(), &project_labels, next.as_ref())?;

        Ok((aggregated, next, row))
    });

    let rows = sort.apply(rows, relation.alias_name())?;

    let rows: SelectIter<'a> = if *distinct {
        let rows = rows.collect::<Result<Vec<_>>>()?;
        let rows = apply_distinct(rows);
        Box::new(rows.into_iter().map(Ok))
    } else {
        rows
    };
    let labels = labels.iter().cloned().collect();

    limit::apply(query, rows).map(|rows| (labels, rows))
}

pub fn select<'a, T>(
    storage: &'a T,
    query: &'a QueryPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<SelectIter<'a>>
where
    T: GStore,
{
    select_with_labels(storage, query, filter_context).map(|(_, rows)| rows)
}
