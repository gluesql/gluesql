use {
    super::{
        aggregate::accumulator::{AggrValue, empty_value},
        context::{AggregateContext, RowContext, WindowValues},
        evaluate::evaluate,
        sort::sort_by,
    },
    crate::{
        data::{Key, Value},
        plan::{
            AggregateFunctionPlan, AggregatePlan, CountArgExprPlan, OrderByExprPlan,
            WindowFunctionPlan, WindowPlan,
        },
        result::Result,
        store::GStore,
    },
    std::{collections::HashMap, rc::Rc},
};

pub type WindowIter<'a> =
    Box<dyn Iterator<Item = Result<(AggregateContext<'a>, Option<Rc<WindowValues>>)>> + 'a>;

/// Computes window function results between `aggregate::apply` and
/// projection. Rows are materialized so every window slot can see the whole
/// partition, but the output preserves the original input row order -
/// windowing never reorders rows; only a query-level `ORDER BY` does.
pub fn apply<'a, T: GStore>(
    storage: &'a T,
    window_slots: Option<&'a [WindowPlan]>,
    filter_context: Option<&Rc<RowContext<'a>>>,
    rows: Box<dyn Iterator<Item = Result<AggregateContext<'a>>> + 'a>,
) -> Result<WindowIter<'a>> {
    let window_slots = window_slots.unwrap_or(&[]);

    if window_slots.is_empty() {
        return Ok(Box::new(rows.map(|context| Ok((context?, None)))));
    }

    let rows = rows.collect::<Result<Vec<_>>>()?;
    let row_contexts = rows
        .iter()
        .map(|context| match (&context.next, filter_context) {
            (Some(next), Some(filter_context)) => Some(Rc::new(RowContext::concat(
                Rc::clone(next),
                Rc::clone(filter_context),
            ))),
            (Some(next), None) => Some(Rc::clone(next)),
            (None, Some(filter_context)) => Some(Rc::clone(filter_context)),
            (None, None) => None,
        })
        .collect::<Vec<_>>();

    let mut slot_values = vec![vec![Value::Null; window_slots.len()]; rows.len()];

    for (slot, window) in window_slots.iter().enumerate() {
        let values = compute_slot(storage, window, &row_contexts)?;

        for (row_index, value) in values.into_iter().enumerate() {
            slot_values[row_index][slot] = value;
        }
    }

    let windowed = slot_values
        .into_iter()
        .map(|values| Some(Rc::new(WindowValues::new(values))));

    let result = rows
        .into_iter()
        .zip(windowed)
        .map(Ok)
        .collect::<Vec<Result<_>>>();

    Ok(Box::new(result.into_iter()))
}

fn compute_slot<'a, T: GStore>(
    storage: &'a T,
    window: &WindowPlan,
    row_contexts: &[Option<Rc<RowContext<'a>>>],
) -> Result<Vec<Value>> {
    let mut result = vec![Value::Null; row_contexts.len()];
    let mut partitions: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();

    for (row_index, context) in row_contexts.iter().enumerate() {
        let key = window
            .over
            .partition_by
            .iter()
            .map(|expr| evaluate(storage, context.as_ref(), None, None, expr)?.try_into())
            .collect::<Result<Vec<Value>>>()?;

        partitions.entry(key).or_default().push(row_index);
    }

    for indices in partitions.into_values() {
        compute_partition(storage, window, row_contexts, &indices, &mut result)?;
    }

    Ok(result)
}

fn compute_partition<'a, T: GStore>(
    storage: &'a T,
    window: &WindowPlan,
    row_contexts: &[Option<Rc<RowContext<'a>>>],
    indices: &[usize],
    result: &mut [Value],
) -> Result<()> {
    let has_order_by = !window.over.order_by.is_empty();

    let ordered = if has_order_by {
        let mut paired = indices
            .iter()
            .map(|&row_index| {
                let context = row_contexts[row_index].as_ref();
                let keys = window
                    .over
                    .order_by
                    .iter()
                    .map(|OrderByExprPlan { expr, asc }| {
                        let value: Value =
                            evaluate(storage, context, None, None, expr)?.try_into()?;

                        Key::try_from(value).map(|key| (key, *asc))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok((row_index, keys))
            })
            .collect::<Result<Vec<_>>>()?;

        paired.sort_by(|(_, a), (_, b)| sort_by(a, b));

        paired
    } else {
        indices
            .iter()
            .map(|&row_index| (row_index, Vec::new()))
            .collect()
    };

    // A peer group is a maximal run of consecutive rows (in window order)
    // that are equal on every ORDER BY expression. With no ORDER BY, the
    // whole partition is a single peer group.
    let mut peer_group = Vec::with_capacity(ordered.len());
    let mut current_group = 0;

    for position in 0..ordered.len() {
        if position > 0 {
            let (_, keys) = &ordered[position];
            let (_, previous_keys) = &ordered[position - 1];
            let same = keys
                .iter()
                .map(|(key, _)| key)
                .eq(previous_keys.iter().map(|(key, _)| key));

            if !same {
                current_group += 1;
            }
        }

        peer_group.push(current_group);
    }

    let row_at = |position: usize| ordered[position].0;

    match &window.func {
        WindowFunctionPlan::RowNumber => {
            for (position, &(row_index, _)) in ordered.iter().enumerate() {
                result[row_index] = Value::I64((position + 1) as i64);
            }
        }
        WindowFunctionPlan::Rank => {
            let mut first_position_of_group: HashMap<usize, usize> = HashMap::new();

            for (position, &group) in peer_group.iter().enumerate() {
                first_position_of_group.entry(group).or_insert(position);
            }

            for (position, &(row_index, _)) in ordered.iter().enumerate() {
                let group = peer_group[position];
                result[row_index] = Value::I64((first_position_of_group[&group] + 1) as i64);
            }
        }
        WindowFunctionPlan::DenseRank => {
            for (position, &(row_index, _)) in ordered.iter().enumerate() {
                result[row_index] = Value::I64((peer_group[position] + 1) as i64);
            }
        }
        WindowFunctionPlan::Lag {
            expr,
            offset,
            default,
        }
        | WindowFunctionPlan::Lead {
            expr,
            offset,
            default,
        } => {
            let is_lag = matches!(window.func, WindowFunctionPlan::Lag { .. });

            for position in 0..ordered.len() {
                let row_index = row_at(position);
                let context = row_contexts[row_index].as_ref();
                let offset_value: Value =
                    evaluate(storage, context, None, None, offset)?.try_into()?;
                let offset_value = match offset_value {
                    Value::I64(n) => usize::try_from(n).unwrap_or(0),
                    _ => 0,
                };

                let target = if is_lag {
                    position.checked_sub(offset_value)
                } else {
                    let target = position + offset_value;
                    (target < ordered.len()).then_some(target)
                };

                result[row_index] = match target {
                    Some(target) => {
                        let target_context = row_contexts[row_at(target)].as_ref();
                        evaluate(storage, target_context, None, None, expr)?.try_into()?
                    }
                    None => match default {
                        Some(default) => {
                            evaluate(storage, context, None, None, default)?.try_into()?
                        }
                        None => Value::Null,
                    },
                };
            }
        }
        WindowFunctionPlan::Aggregate(aggregate) => {
            if has_order_by {
                let mut position = 0;
                let mut accumulator: Option<AggrValue> = None;

                while position < ordered.len() {
                    let group = peer_group[position];
                    let group_end = ordered.len().min(
                        position
                            + peer_group[position..]
                                .iter()
                                .take_while(|&&g| g == group)
                                .count(),
                    );

                    for row_index in (position..group_end).map(row_at) {
                        let context = row_contexts[row_index].as_ref();
                        let value = aggregate_input(storage, aggregate, context)?;

                        match accumulator.as_mut() {
                            Some(accumulator) => {
                                accumulator.accumulate(&value)?;
                            }
                            None => accumulator = Some(AggrValue::new(aggregate, &value)?),
                        }
                    }

                    let exported = match &accumulator {
                        Some(accumulator) => accumulator.clone().export()?,
                        None => empty_value(aggregate),
                    };

                    for row_index in (position..group_end).map(row_at) {
                        result[row_index] = exported.clone();
                    }

                    position = group_end;
                }
            } else {
                let mut accumulator: Option<AggrValue> = None;

                for &row_index in indices {
                    let context = row_contexts[row_index].as_ref();
                    let value = aggregate_input(storage, aggregate, context)?;

                    match accumulator.as_mut() {
                        Some(accumulator) => {
                            accumulator.accumulate(&value)?;
                        }
                        None => accumulator = Some(AggrValue::new(aggregate, &value)?),
                    }
                }

                let exported = match accumulator {
                    Some(accumulator) => accumulator.export()?,
                    None => empty_value(aggregate),
                };

                for &row_index in indices {
                    result[row_index] = exported.clone();
                }
            }
        }
    }

    Ok(())
}

fn aggregate_input<'a, T: GStore>(
    storage: &'a T,
    aggregate: &AggregatePlan,
    context: Option<&Rc<RowContext<'a>>>,
) -> Result<Value> {
    match &aggregate.func {
        AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard) => Ok(Value::Null),
        AggregateFunctionPlan::Count(CountArgExprPlan::Expr(expr))
        | AggregateFunctionPlan::Sum(expr)
        | AggregateFunctionPlan::Min(expr)
        | AggregateFunctionPlan::Max(expr)
        | AggregateFunctionPlan::Avg(expr)
        | AggregateFunctionPlan::Variance(expr)
        | AggregateFunctionPlan::Stdev(expr) => {
            evaluate(storage, context, None, None, expr)?.try_into()
        }
    }
}
