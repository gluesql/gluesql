use {
    super::{
        context::{AggregateContext, RowContext},
        evaluate::{EvaluateError, evaluate},
        filter::check_expr,
    },
    crate::{
        ast::{Aggregate, AggregateFunction, CountArgExpr, Expr, SelectItem},
        data::Value,
        result::Result,
        store::GStore,
    },
    futures::{
        TryStreamExt,
        stream::{self, Stream},
    },
    std::{
        cmp::Ordering,
        collections::{HashMap, HashSet, hash_map::Entry},
        sync::Arc,
    },
};

#[derive(futures_enum::Stream)]
enum S<T1, T2> {
    NonAggregate(T1),
    Aggregate(T2),
}

pub async fn apply<'a, T, U>(
    storage: &'a T,
    fields: &'a [SelectItem],
    group_by: &'a [Expr],
    having: Option<&'a Expr>,
    filter_context: Option<Arc<RowContext<'a>>>,
    rows: U,
) -> Result<impl Stream<Item = Result<AggregateContext<'a>>> + use<'a, T, U>>
where
    T: GStore,
    U: futures::Stream<Item = Result<Arc<RowContext<'a>>>> + 'a,
{
    if !check_aggregate(fields, group_by) {
        let rows = rows.map_ok(|project_context| AggregateContext {
            aggregated: None,
            next: project_context,
        });
        return Ok(S::NonAggregate(rows));
    }

    let aggregates = collect_aggregates(fields, having);
    let filter_context_ref = &filter_context;
    let group_by_ref = group_by;

    let accumulator = rows
        .into_stream()
        .try_fold(GroupAccumulator::new(), |mut acc, project_context| {
            let aggregates_ref = &aggregates;
            async move {
                let row_filter_context = Some(match filter_context_ref.as_ref() {
                    Some(filter) => Arc::new(RowContext::concat(
                        Arc::clone(&project_context),
                        Arc::clone(filter),
                    )),
                    None => Arc::clone(&project_context),
                });

                let mut group_key = Vec::with_capacity(group_by_ref.len());
                for expr in group_by_ref {
                    let context = row_filter_context.as_ref().map(Arc::clone);
                    let value = evaluate(storage, context, None, expr).await?.try_into()?;
                    group_key.push(value);
                }

                let group_state = match acc.groups.entry(group_key.clone()) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        acc.order.push(group_key.clone());
                        entry.insert(GroupState::new(Arc::clone(&project_context)))
                    }
                };

                if !aggregates_ref.is_empty() {
                    for &aggregate in aggregates_ref {
                        let context = row_filter_context.as_ref().map(Arc::clone);
                        let value = evaluate_aggregate_value(storage, context, aggregate).await?;
                        group_state.update(aggregate, &value)?;
                    }
                }

                Ok(acc)
            }
        })
        .await?;

    let contexts = finalize_groups(
        storage,
        filter_context_ref,
        having,
        &aggregates,
        accumulator,
    )
    .await?;
    let rows = stream::iter(contexts.into_iter().map(Ok));

    Ok(S::Aggregate(rows))
}

fn check_aggregate(fields: &[SelectItem], group_by: &[Expr]) -> bool {
    if !group_by.is_empty() {
        return true;
    }

    fields.iter().any(|field| match field {
        SelectItem::Expr { expr, .. } => expr.contains_aggregate(),
        _ => false,
    })
}

fn collect_aggregates<'a>(
    fields: &'a [SelectItem],
    having: Option<&'a Expr>,
) -> Vec<&'a Aggregate> {
    let mut seen: HashSet<&'a Aggregate> = HashSet::new();
    let mut aggregates = Vec::new();

    let mut push = |aggregate: &'a Aggregate| {
        if seen.insert(aggregate) {
            aggregates.push(aggregate);
        }
    };

    for field in fields {
        if let SelectItem::Expr { expr, .. } = field {
            expr.visit_aggregates(&mut push);
        }
    }

    if let Some(expr) = having {
        expr.visit_aggregates(&mut push);
    }

    aggregates
}

struct GroupAccumulator<'a> {
    order: Vec<Vec<Value>>,
    groups: HashMap<Vec<Value>, GroupState<'a>>,
}

impl GroupAccumulator<'_> {
    fn new() -> Self {
        Self {
            order: Vec::new(),
            groups: HashMap::new(),
        }
    }
}

struct GroupState<'a> {
    context: Arc<RowContext<'a>>,
    aggregates: HashMap<&'a Aggregate, AggrValue>,
}

impl<'a> GroupState<'a> {
    fn new(context: Arc<RowContext<'a>>) -> Self {
        Self {
            context,
            aggregates: HashMap::new(),
        }
    }

    fn update(&mut self, aggregate: &'a Aggregate, value: &Value) -> Result<()> {
        if let Some(existing) = self.aggregates.get_mut(aggregate) {
            existing.accumulate(value)
        } else {
            let aggr_value = AggrValue::new(aggregate, value)?;
            self.aggregates.insert(aggregate, aggr_value);
            Ok(())
        }
    }
}

async fn finalize_groups<'a, T: GStore>(
    storage: &'a T,
    filter_context: &Option<Arc<RowContext<'a>>>,
    having: Option<&'a Expr>,
    aggregates: &[&'a Aggregate],
    mut accumulator: GroupAccumulator<'a>,
) -> Result<Vec<AggregateContext<'a>>> {
    let mut contexts = Vec::with_capacity(accumulator.order.len());

    for group_key in accumulator.order {
        let Some(mut state) = accumulator.groups.remove(&group_key) else {
            continue;
        };

        let aggregated = if aggregates.is_empty() {
            None
        } else {
            let mut map = HashMap::with_capacity(aggregates.len());
            for &aggregate in aggregates {
                let value = match state.aggregates.remove(aggregate) {
                    Some(value) => value.finalize()?,
                    None => default_aggregate_result(aggregate),
                };
                map.insert(aggregate, value);
            }
            Some(map)
        };

        if let Some(having_expr) = having {
            let combined = match filter_context.as_ref() {
                Some(filter) => Arc::new(RowContext::concat(
                    Arc::clone(&state.context),
                    Arc::clone(filter),
                )),
                None => Arc::clone(&state.context),
            };

            let aggregated_arc = aggregated.as_ref().map(|map| Arc::new(map.clone()));

            if !check_expr(storage, Some(combined), aggregated_arc, having_expr).await? {
                continue;
            }
        }

        contexts.push(AggregateContext {
            aggregated,
            next: state.context,
        });
    }

    Ok(contexts)
}

async fn evaluate_aggregate_value<'a, T: GStore>(
    storage: &'a T,
    context: Option<Arc<RowContext<'a>>>,
    aggregate: &'a Aggregate,
) -> Result<Value> {
    match &aggregate.func {
        AggregateFunction::Count(CountArgExpr::Wildcard) => {
            if aggregate.distinct {
                let context = context.ok_or_else(|| {
                    EvaluateError::FilterContextRequiredForAggregate(Box::new(aggregate.clone()))
                })?;
                let entries = context.get_all_entries();
                let values = entries.into_iter().map(|(_, value)| value).collect();
                Ok(Value::List(values))
            } else {
                Ok(Value::Null)
            }
        }
        AggregateFunction::Count(CountArgExpr::Expr(expr))
        | AggregateFunction::Sum(expr)
        | AggregateFunction::Min(expr)
        | AggregateFunction::Max(expr)
        | AggregateFunction::Avg(expr)
        | AggregateFunction::Variance(expr)
        | AggregateFunction::Stdev(expr) => {
            evaluate(storage, context, None, expr).await?.try_into()
        }
    }
}

#[derive(Clone)]
enum AggrValue {
    Count {
        wildcard: bool,
        count: i64,
        distinct_values: Option<HashSet<Value>>,
    },
    Sum {
        value: Value,
        distinct_values: Option<HashSet<Value>>,
    },
    Min {
        value: Value,
        distinct_values: Option<HashSet<Value>>,
    },
    Max {
        value: Value,
        distinct_values: Option<HashSet<Value>>,
    },
    Avg {
        sum: Value,
        count: i64,
        distinct_values: Option<HashSet<Value>>,
    },
    Variance {
        sum_square: Value,
        sum: Value,
        count: i64,
        distinct_values: Option<HashSet<Value>>,
    },
    Stdev {
        sum_square: Value,
        sum: Value,
        count: i64,
        distinct_values: Option<HashSet<Value>>,
    },
}

impl AggrValue {
    fn new(aggregate: &Aggregate, value: &Value) -> Result<Self> {
        let value = value.clone();

        Ok(match &aggregate.func {
            AggregateFunction::Count(CountArgExpr::Wildcard) => {
                let distinct_values = if aggregate.distinct {
                    let mut set = HashSet::new();
                    set.insert(value.clone());
                    Some(set)
                } else {
                    None
                };

                AggrValue::Count {
                    wildcard: true,
                    count: 1,
                    distinct_values,
                }
            }
            AggregateFunction::Count(CountArgExpr::Expr(_)) => {
                let mut distinct_values = if aggregate.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if !value.is_null()
                    && let Some(set) = distinct_values.as_mut()
                {
                    set.insert(value.clone());
                }

                AggrValue::Count {
                    wildcard: false,
                    count: i64::from(!value.is_null()),
                    distinct_values,
                }
            }
            AggregateFunction::Sum(_) => {
                let mut distinct_values = if aggregate.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Sum {
                    value,
                    distinct_values,
                }
            }
            AggregateFunction::Min(_) => {
                let mut distinct_values = if aggregate.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Min {
                    value,
                    distinct_values,
                }
            }
            AggregateFunction::Max(_) => {
                let mut distinct_values = if aggregate.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Max {
                    value,
                    distinct_values,
                }
            }
            AggregateFunction::Avg(_) => {
                let mut distinct_values = if aggregate.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Avg {
                    sum: value,
                    count: 1,
                    distinct_values,
                }
            }
            AggregateFunction::Variance(_) => {
                let mut distinct_values = if aggregate.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Variance {
                    sum_square: value.multiply(&value)?,
                    sum: value,
                    count: 1,
                    distinct_values,
                }
            }
            AggregateFunction::Stdev(_) => {
                let mut distinct_values = if aggregate.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Stdev {
                    sum_square: value.multiply(&value)?,
                    sum: value,
                    count: 1,
                    distinct_values,
                }
            }
        })
    }

    fn accumulate(&mut self, new_value: &Value) -> Result<()> {
        match self {
            AggrValue::Count {
                wildcard,
                count,
                distinct_values,
            } => {
                let should_process = if new_value.is_null() {
                    true
                } else {
                    check_distinct(distinct_values, new_value)
                };

                if should_process && (*wildcard || !new_value.is_null()) {
                    *count += 1;
                }

                Ok(())
            }
            AggrValue::Sum {
                value,
                distinct_values,
            } => {
                if check_distinct(distinct_values, new_value) {
                    *value = value.add(new_value)?;
                }
                Ok(())
            }
            AggrValue::Min {
                value,
                distinct_values,
            } => {
                if check_distinct(distinct_values, new_value)
                    && value
                        .evaluate_cmp(new_value)
                        .is_some_and(|ordering| ordering == Ordering::Greater)
                {
                    *value = new_value.clone();
                }
                Ok(())
            }
            AggrValue::Max {
                value,
                distinct_values,
            } => {
                if check_distinct(distinct_values, new_value)
                    && value
                        .evaluate_cmp(new_value)
                        .is_some_and(|ordering| ordering == Ordering::Less)
                {
                    *value = new_value.clone();
                }
                Ok(())
            }
            AggrValue::Avg {
                sum,
                count,
                distinct_values,
            } => {
                if check_distinct(distinct_values, new_value) {
                    *sum = sum.add(new_value)?;
                    *count += 1;
                }
                Ok(())
            }
            AggrValue::Variance {
                sum_square,
                sum,
                count,
                distinct_values,
            } => {
                if check_distinct(distinct_values, new_value) {
                    *sum_square = sum_square.add(&new_value.multiply(new_value)?)?;
                    *sum = sum.add(new_value)?;
                    *count += 1;
                }
                Ok(())
            }
            AggrValue::Stdev {
                sum_square,
                sum,
                count,
                distinct_values,
            } => {
                if check_distinct(distinct_values, new_value) {
                    *sum_square = sum_square.add(&new_value.multiply(new_value)?)?;
                    *sum = sum.add(new_value)?;
                    *count += 1;
                }
                Ok(())
            }
        }
    }

    fn finalize(&self) -> Result<Value> {
        use crate::ast::DataType;

        let variance = |sum_square: &Value, sum: &Value, count: i64| -> Result<Value> {
            let count_value = Value::I64(count);
            let sum_expr1 = sum_square.multiply(&count_value)?;
            let sum_expr2 = sum.multiply(sum)?;
            let expr_sub = sum_expr1.cast(&DataType::Float)?.subtract(&sum_expr2)?;
            let count_square = count_value.multiply(&count_value)?;
            expr_sub.divide(&count_square)
        };

        match self {
            AggrValue::Count { count, .. } => Ok(Value::I64(*count)),
            AggrValue::Sum { value, .. }
            | AggrValue::Min { value, .. }
            | AggrValue::Max { value, .. } => Ok(value.clone()),
            AggrValue::Avg { sum, count, .. } => {
                let sum = sum.cast(&DataType::Float)?;
                sum.divide(&Value::I64(*count))
            }
            AggrValue::Variance {
                sum_square,
                sum,
                count,
                ..
            } => variance(sum_square, sum, *count),
            AggrValue::Stdev {
                sum_square,
                sum,
                count,
                ..
            } => variance(sum_square, sum, *count)?.sqrt(),
        }
    }
}

fn check_distinct(distinct_values: &mut Option<HashSet<Value>>, new_value: &Value) -> bool {
    match distinct_values {
        Some(set) => {
            if set.contains(new_value) {
                false
            } else {
                set.insert(new_value.clone());
                true
            }
        }
        None => true,
    }
}

fn default_aggregate_result(aggregate: &Aggregate) -> Value {
    match aggregate.func {
        AggregateFunction::Count(_) => Value::I64(0),
        _ => Value::Null,
    }
}
